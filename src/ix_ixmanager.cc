/*
 * ix_manager.cc
 *
 *  Created on: May 8, 2018
 *      Author: zyh
 */

#include "ix.h"
#include "status.h"
#include "pf_internal.h"
#include <string>
#include <string.h>

namespace toydb {

IX_Manager::IX_Manager(PF_Manager &pfm) : pfm_(pfm) {

}

IX_Manager::~IX_Manager() {
}

int comparator_int(void *value1, void *value2, int attr_len) {
  if (*(int *)value1 < *(int *)value2) return -1;
  else if (*(int *)value1 > *(int *)value2) return 1;
  else return 0;
}

int comparator_float(void *value1, void *value2, int attr_len) {
  if (*(float *)value1 < *(float *)value2) return -1;
  else if (*(float *)value1 > *(float *)value2) return 1;
  else return 0;
}

int comparator_string(void *value1, void *value2, int attr_len) {
  return strncmp((char *) value1, (char *) value2, attr_len);
}

bool IX_Manager::IsValidIndex(AttrType attr_type, int attr_length) {
  if (attr_type == INT || attr_type == FLOAT) {
    if (attr_length == 4) {
      return true;
    }
  } else if (attr_type == STRING) {
    if (attr_length > 0 && attr_length < kMaxStrLen) {
      return true;
    }
  }
  return false;
}

Status IX_Manager::GetIndexFileName(const char *file_name, int index_num,
                                    std::string &ix_name) {
  if (file_name == nullptr) {
    return Status(ErrorCode::kIX, "file name is null");
  }
  if (index_num < 0) {
    return Status(ErrorCode::kIX, "index num is negative");
  }
  ix_name = std::string(file_name);
  ix_name.append("-");
  ix_name.append(std::to_string(index_num));
  return Status::OK();
}

Status IX_Manager::CreateIndex(const char *rel_id, int index_num,
                               AttrType attr_type, int attr_length) {
  // create file
  Status s;
  std::string index_file_name;
  s = GetIndexFileName(rel_id, index_num, index_file_name);
  s = pfm_.CreateFile(index_file_name.c_str()); if (!s.ok()) return s;

  // create index file header page and root page
  PF_FileHandle pfh;
  Page ix_header_page;
  Page root_page;
  IX_FileHeader *header;
  NodeHeader_L *root_header;
  NodeEntry *entry;
  s = pfm_.OpenFile(index_file_name.c_str(), pfh); if (!s.ok()) return s;
  s = pfh.AllocatePage(ix_header_page); if(!s.ok()) return s;
  s = pfh.AllocatePage(root_page); if(!s.ok()) return s;

  // setup header page
  header = (IX_FileHeader *) ix_header_page.data;
  header->attr_type = attr_type;
  header->attr_length = attr_length;
  // node
  header->max_keys_n = (kPageSize - sizeof(PF_PageHeader) - sizeof(NodeHeader))
                       / (attr_length + sizeof(NodeEntry));
  header->entry_offset_n = sizeof(NodeHeader);
  header->key_offset_n = header->entry_offset_n + header->max_keys_n * sizeof(NodeEntry);
  // bucket
  header->max_keys_b = (kPageSize - sizeof(PF_PageHeader) - sizeof(BucketHeader))
                           / sizeof(BucketEntry);
  header->entry_offset_b = sizeof(BucketHeader);
  header->root_page = root_page.num;
  s = pfh.MarkDirty(ix_header_page.num); if (!s.ok()) return s;
  s = pfh.UnpinPage(ix_header_page.num); if (!s.ok()) return s;

  // setup root page
  root_header = (NodeHeader_L *) root_page.data;
  root_header->is_leaf = true;
  root_header->num_keys = 0;
  root_header->first_slot = -1;
  root_header->free_slot = 0;
  root_header->prev_page = -1;
  root_header->next_page = -1;
  entry = (NodeEntry *)((char *)root_header + header->entry_offset_n);
  for (int i = 0; i < header->max_keys_n; i++) {
    entry[i].next_slot = i + 1;
  }
  entry[header->max_keys_n].next_slot = -1;
  s = pfh.MarkDirty(root_page.num); if (!s.ok()) return s;
  s = pfh.UnpinPage(root_page.num); if (!s.ok()) return s;
  s = pfm_.CloseFile(pfh); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_Manager::DestroyIndex(const char *file_name, int index_num) {
  Status s;
  std::string index_file_name;
  s = GetIndexFileName(file_name, index_num, index_file_name); if (!s.ok()) return s;
  s = pfm_.DeleteFile(index_file_name.c_str()); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_Manager::OpenIndex(const char *rel_id, int index_num,
                             IX_IndexHandle &ixh) {
  Status s;
  std::string index_file_name;
  s = GetIndexFileName(rel_id, index_num, index_file_name); if (!s.ok()) return s;
  PF_FileHandle pfh;
  Page p;
  IX_FileHeader *header;
  s = pfm_.OpenFile(index_file_name.c_str(), pfh); if (!s.ok()) return s;
  s = pfh.GetPage(0, p); if (!s.ok()) return s;
  header = (IX_FileHeader *) p.data;
  if (!IsValidIndex(header->attr_type, header->attr_length)) {
    return Status(ErrorCode::kIX, "index attribute type or length is invalid");
  }
  memcpy(&ixh.header_, header, sizeof(IX_FileHeader));
  s = pfh.UnpinPage(0); if (!s.ok()) return s;
  switch (header->attr_type) {
    case INT: ixh.comparator_ = &comparator_int; break;
    case FLOAT: ixh.comparator_ = &comparator_float; break;
    case STRING: ixh.comparator_ = &comparator_string; break;
    default: return Status(ErrorCode::kIX, "unknown attribute type");
  }
  ixh.header_changed_ = false;
  ixh.pfh_ = pfh;
  ixh.index_open_ = true;
  return Status::OK();
}

Status IX_Manager::CloseIndex(IX_IndexHandle &ixh) {
  Status s;
  if (ixh.index_open_ == false) {
    return Status(ErrorCode::kIX, "can not close index.index file is not open");
  }
  if (ixh.header_changed_ == true) {
    Page p;
    IX_FileHeader *header;
    s = ixh.pfh_.GetPage(0, p); if (!s.ok()) return s;
    header = (IX_FileHeader *) p.data;
    memcpy(header, &ixh.header_, sizeof(IX_FileHeader));
    s = ixh.pfh_.MarkDirty(p.num); if (!s.ok()) return s;
    s = ixh.pfh_.UnpinPage(p.num); if (!s.ok()) return s;
  }
  s = pfm_.CloseFile(ixh.pfh_); if (!s.ok()) return s;
  ixh.index_open_ = false;
  return Status::OK();
}

}  // namespace toydb
