/*
 * ix_indexhandle.cc
 *
 *  Created on: May 8, 2018
 *      Author: zyh
 */

#include "ix.h"
#include <string.h>
#include <stdlib.h>

namespace toydb {

IX_IndexHandle::IX_IndexHandle() {
  index_open_ = false;
  scan_start_ = false;
  bucket_open_ = false;
  header_changed_ = false;
}

IX_IndexHandle::~IX_IndexHandle() {

}

Status IX_IndexHandle::InsertEntry(void *data, const RID &rid) {
  Status s;
  if (index_open_ == false) {
    return Status(ErrorCode::kRM, "can not insert entry. index is not open");
  }
  if (data == nullptr) {
    return Status(ErrorCode::kRM, "can not insert data. data is null");
  }
  Page p;
  NodeHeader *header;
  s = pfh_.GetPage(header_.root_page, p); if (!s.ok()) return s;
  header = (NodeHeader *) p.data;
  if (header->num_keys < header_.max_keys_n) {  // if root page is empty
    s = InsertIntoNonFullNode(header, rid, data); if (!s.ok()) return s;
  } else {  // if root page is full
    PageNum new_root_page;
    Page new_root_p;
    NodeHeader_I *new_root_header;
    PageNum new_node_page;
    int new_root_index;
    s = CreateNode(new_root_page);
    s = pfh_.GetPage(new_root_page, new_root_p);
    new_root_header = (NodeHeader_I *) new_root_p.data;
    new_root_header->is_leaf = false;
    new_root_header->first_page = header_.root_page;
    s = SplitNode((NodeHeader *)new_root_header, header, header_.root_page, -1, new_node_page,
                  new_root_index);
    s = pfh_.MarkDirty(header_.root_page); if (!s.ok()) return s;
    s = pfh_.UnpinPage(header_.root_page); if (!s.ok()) return s;
    s = InsertIntoNonFullNode((NodeHeader *)new_root_header, rid, data); if (!s.ok()) return s;
    header_.root_page = new_root_page;
    header_changed_ = true;
  }
  s = pfh_.MarkDirty(header_.root_page); if (!s.ok()) return s;
  s = pfh_.UnpinPage(header_.root_page); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_IndexHandle::DeleteEntry(void *value, const RID &rid) {
  Status s;
  if (index_open_ == false) {
    return Status(ErrorCode::kIX, "can not delete entry. index is not open");
  }
  Page root_p;
  s = pfh_.GetPage(header_.root_page, root_p); if (!s.ok()) return s;
  NodeHeader *root_header = (NodeHeader *) root_p.data;
  bool empty;
  bool dirty;
  if (root_header->is_leaf == true) {
    if (root_header->num_keys == 0) {
      return Status(ErrorCode::kIX, "can not delete entry. root node is empty");
    }
    DeleteFromLeafNode((NodeHeader_L *) root_header, value, rid, empty, dirty);
  } else {
    DeleteFromInternalNode((NodeHeader_I *) root_header, value, rid, empty, dirty);
  }
  if (empty) {
    root_header->is_leaf = true;
    dirty = true;
  }
  if (dirty) {
    s = pfh_.MarkDirty(header_.root_page); if (!s.ok()) return s;
  }
  s = pfh_.UnpinPage(header_.root_page); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_IndexHandle::ForcePages() {
  Status s;
  if (!index_open_) {
    return Status(ErrorCode::kIX, "can not force page. index is not open");
  }
  s = pfh_.ForcePages(); if (!s.ok()) return s;
  if (header_changed_) {
    Page p;
    s = pfh_.GetPage(0, p); if (!s.ok()) return s;
    IX_FileHeader *header = (IX_FileHeader *) p.data;
    memcpy(header, &header_, sizeof(IX_FileHeader));
    s = pfh_.UnpinPage(0); if (!s.ok()) return s;
    header_changed_ = false;
  }
  return Status::OK();
}

Status IX_IndexHandle::InsertIntoNonFullNode(NodeHeader *node_header,
                                             const RID &rid, void *value) {
  Status s;

  int index;
  int unused;
  bool is_dup;
  FindIndexInNode(node_header, value, index, unused, is_dup);

  NodeEntry *entry = (NodeEntry *) ((char *) node_header + header_.entry_offset_n);
  char *keys = (char *) node_header + header_.key_offset_n;

  if (node_header->is_leaf) {  // it is a leaf node
    if (!is_dup) {
      int slot = node_header->free_slot;
      node_header->free_slot = entry[slot].next_slot;
      entry[slot].is_duplicate = false;
      entry[slot].page = rid.page_num;
      entry[slot].slot = rid.slot_num;
      memcpy(keys + slot * header_.attr_length, value, header_.attr_length);
      if (index == -1) {
        entry[slot].next_slot = node_header->first_slot;
        node_header->first_slot = slot;
      } else {
        entry[slot].next_slot = entry[index].next_slot;
        entry[index].next_slot = slot;
      }
      node_header->num_keys++;
    } else {
      if (entry[index].is_duplicate == false) {
        PageNum page_num;
        s = CreateBucket(page_num); if (!s.ok()) return s;
        InsertIntoBucket(page_num, rid);
        RID old_rid(entry[index].page,entry[index].slot);
        s = InsertIntoBucket(page_num, old_rid); if (!s.ok()) return s;
        entry[index].is_duplicate = true;
        entry[index].page = page_num;
        entry[index].slot = -1;
      } else {
        s = InsertIntoBucket(entry[index].page, rid); return s;
      }
    }
  } else {  // it is a internal node
    PageNum next_node_page;
    if (index == -1) {
      next_node_page = ((NodeHeader_I *)node_header)->first_page;
    } else {
      next_node_page = entry[index].page;
    }
    NodeHeader *next_node_header;
    Page next_p;
    PageNum insert_page;
    NodeHeader *insert_header;
    s = pfh_.GetPage(next_node_page, next_p); if (!s.ok()) return s;
    next_node_header = (NodeHeader *)next_p.data;
    insert_page = next_node_page;
    insert_header = next_node_header;
    if (next_node_header->num_keys == header_.max_keys_n) {
      int new_parent_index;
      int new_page;
      Page new_p;
      s = SplitNode(node_header, next_node_header, next_node_page, index,
                    new_parent_index, new_page);
      if (!s.ok()) return s;
      char *new_index_value = keys + new_parent_index * header_.attr_length;
      int comp = comparator_(value, (void *)new_index_value, header_.attr_length);
      if (comp >= 0) {
        s = pfh_.MarkDirty(next_node_page); if(!s.ok()) return s;
        s = pfh_.UnpinPage(next_node_page); if(!s.ok()) return s;
        s = pfh_.GetPage(new_page, new_p);
        insert_page = new_page;
        insert_header = (NodeHeader *)new_p.data;
      }
    }
    s = InsertIntoNonFullNode(insert_header, rid, value);
    s = pfh_.MarkDirty(insert_page); if(!s.ok()) return s;
    s = pfh_.UnpinPage(insert_page); if(!s.ok()) return s;
  }
  return Status::OK();
}

Status IX_IndexHandle::DeleteFromLeafNode(NodeHeader_L *header, void *value,
                                          const RID &rid, bool &empty, bool &dirty) {
  Status s;
  int index;
  int prev_index;
  bool is_dup;
  empty = false;
  dirty = false;
  FindIndexInNode((NodeHeader *)header, value, index, prev_index, is_dup);
  if (is_dup == false) {
    return Status(ErrorCode::kIX,
                  "can not delete entry from leaf. the value does not exist");
  }
  NodeEntry *entry = (NodeEntry *)((char *)header + header_.entry_offset_n);
  if (entry[index].is_duplicate == true) {
    bool is_last_rid;
    RID last_rid;
    PageNum new_first_bucket;
    bool new_bucket;
    s = DeleteFromBucketList(entry[index].page, rid, is_last_rid,
                         last_rid, new_first_bucket, new_bucket);
    if(!s.ok()) return s;
    if (is_last_rid) {
      entry[index].page = last_rid.page_num;
      entry[index].slot = last_rid.slot_num;
      dirty = true;
    }
    if (new_bucket) {
      entry[index].page = new_first_bucket;
      dirty = true;
    }
  } else {
    if (!(rid.page_num == entry[index].page && rid.slot_num == entry[index].slot)) {
      return Status(ErrorCode::kIX,
                    "can not delete entry from leaf. the rid does not exist");
    }
    entry[prev_index].next_slot = entry[index].next_slot;
    entry[index].next_slot = header->free_slot;
    header->free_slot = index;
    header->num_keys--;
    if (header->num_keys == 0) empty = true;
    dirty = true;
  }
  return Status::OK();
}

Status IX_IndexHandle::DeleteFromInternalNode(NodeHeader_I *header, void *value, const RID &rid,
                                bool &empty, bool &dirty) {
  Status s;
  empty = false;
  int index;
  int prev_index;
  bool is_dup;
  PageNum next_node_page;
  FindIndexInNode((NodeHeader *)header, value, index, prev_index, is_dup);
  NodeEntry *entry = (NodeEntry *)((char *)header + header_.entry_offset_n);
  if (index == -1) next_node_page = header->first_page;
  else  next_node_page = entry[index].page;
  Page next_p;
  s = pfh_.GetPage(next_node_page, next_p);
  NodeHeader *next_header = (NodeHeader *) next_p.data;
  bool next_empty;
  bool next_dirty;
  if (next_header->is_leaf == true) {
    DeleteFromLeafNode((NodeHeader_L *) next_header, value, rid, next_empty, next_dirty);
  } else {
    DeleteFromInternalNode((NodeHeader_I *) next_header, value, rid, next_empty, next_dirty);
  }
  if (next_dirty) {
    s = pfh_.MarkDirty(next_node_page); if (!s.ok()) return s;
  }
  s = pfh_.UnpinPage(next_node_page); if (!s.ok()) return s;
  if (next_empty) {
    s = pfh_.DisposePage(next_node_page); if (!s.ok()) return s;
    if (index == -1) {
      int first_slot = header->first_slot;
      header->first_slot = entry[first_slot].next_slot;
      header->first_page = entry[first_slot].page;
      entry[first_slot].next_slot = header->free_slot;
      header->free_slot = first_slot;
    } else {
      if (prev_index != -1)
        entry[prev_index].next_slot = entry[index].next_slot;
      else
        header->first_slot = index;
      entry[index].next_slot = header->free_slot;
      header->free_slot = index;
    }
    if (header->num_keys == 0) {
      header->first_page = -1;
      empty = true;
    } else {
      header->num_keys--;
    }
    dirty = true;
  }
  return Status::OK();
}

Status IX_IndexHandle::GoToFirstEntry() {
  Status s;
  PageNum curr_page = header_.root_page;
  Page p;
  NodeHeader *header;
  leaf_open_ = true;

  // find first leaf page
  while (1) {
    s = pfh_.GetPage(curr_page, p); if (!s.ok()) return s;
    header = (NodeHeader *) p.data;
    if (header->is_leaf) break;
    NodeHeader_I *header_I = (NodeHeader_I *) header;
    PageNum next_page = header_I->first_page;
    s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
    curr_page = next_page;
  }

  if (curr_leaf_header_->first_slot == -1) {  // only if root node is empty
    s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
    leaf_open_ = false;
    return Status::OK();
  }

  curr_leaf_num_ = curr_page;
  curr_leaf_header_ = (NodeHeader_L *) p.data;
  leaf_slot_num_ = curr_leaf_header_->first_slot;
  leaf_entry_ = (NodeEntry *)((char *)curr_leaf_header_ + header_.entry_offset_n);
  key_ = (char *)curr_leaf_header_ + header_.key_offset_n;

  // if node entry is valid and has a bucket, open the bucket
  if (leaf_entry_[leaf_slot_num_].is_duplicate) {  // if the entry has a bucket
    OpenFirstBucket();
  } else {
    bucket_open_ = false;
  }
  scan_start_ = true;
  first_entry_ = true;
  return Status::OK();
}

// find first entry that is greater (or equal)
Status IX_IndexHandle::GoToAppropriateEntry(bool equal, void *value) {
  Status s;
  PageNum next_page;
  PageNum curr_page = header_.root_page;
  Page p;
  NodeHeader *header;
  NodeEntry *entry;
  int index;
  int prev_index;
  bool is_dup;
  leaf_open_ = true;

  // find the appropriate leaf
  while (1) {
    s = pfh_.GetPage(curr_page, p); if (!s.ok()) return s;
    header = (NodeHeader *) p.data;
    if (header->is_leaf) break;
    FindIndexInNode(header, value, index, prev_index, is_dup);
    NodeHeader_I *header_I = (NodeHeader_I *) header;
    entry = (NodeEntry *) ((char *) header + header_.entry_offset_n);
    if (index != -1) {
      next_page = entry[index].page;
    } else {
      next_page = header_I->first_page;
    }
    s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
    curr_page = next_page;
  }

  bool found = false;
  while(1) {
    NodeHeader_L *header_L = (NodeHeader_L *) header;
    int curr_idx = header_L->first_slot;
    NodeEntry *entry = (NodeEntry *) ((char *)header_L + header_.entry_offset_n);
    char *key = (char *) header_L + header_.key_offset_n;
    while (curr_idx != -1) {
      char *data = key + curr_idx * header_.attr_length;
      int comp = comparator_(value, (void *) data, header_.attr_length);
      if (comp < 0 || (equal && comp == 0)) {
        found = true;
        break;
      }
      curr_idx = entry[curr_idx].next_slot;
    }
    if (found) {
      curr_leaf_num_ = curr_page;
      curr_leaf_header_ = header_L;
      leaf_entry_ = (NodeEntry *)((char *)curr_leaf_header_ + header_.entry_offset_n);
      key_ = (char *)curr_leaf_header_ + header_.key_offset_n;
      leaf_slot_num_ = curr_idx;
      if (leaf_entry_[leaf_slot_num_].is_duplicate) {
        s = OpenFirstBucket(); if (!s.ok()) return s;
      } else {
        bucket_open_ = false;
      }
      first_entry_ = true;
      break;
    } else {  // go to next leaf node
      next_page = header_L->next_page;
      s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
      if (next_page == -1) {
        leaf_open_ = false;
        break;
      }
      s = pfh_.GetPage(next_page, p); if (!s.ok()) return s;
      curr_page = next_page;
      header = (NodeHeader *) p.data;
    }
  }
  scan_start_ = true;
  return Status::OK();
}

Status IX_IndexHandle::OpenFirstBucket() {
  Status s;
  Page bucket_p;
  curr_bucket_num_ = leaf_entry_[leaf_slot_num_].page;
  s = pfh_.GetPage(curr_bucket_num_, bucket_p); if (!s.ok()) return s;
  curr_bucket_header_ = (BucketHeader *)bucket_p.data;
  bucket_slot_num_ = curr_bucket_header_->first_slot;
  bucket_entry_ = (BucketEntry *) ((char *) curr_bucket_header_
      + header_.entry_offset_b);
  bucket_open_ = true;
  return Status::OK();
}

Status IX_IndexHandle::GetNextRIDAndValue(RID &rid, void *&value, bool &eof) {
  Status s;
  if (!index_open_) {
    return Status(ErrorCode::kIX, "can not get next entry. index handle is not open");
  }
  if (!scan_start_) {
    return Status(ErrorCode::kIX, "can not get next entry. scan is not initialized");
  }
  if (!leaf_open_) {
    eof = true;
    return Status::OK();
  }

  if (!first_entry_) {  // if it is not the first entry, move a step
    bool entry_found = false;
    if (bucket_open_) {  // if bucket is open
      int next_slot = bucket_entry_[bucket_slot_num_].next_slot;
      if (next_slot != -1) {  // if next entry is still in this bucket
        bucket_slot_num_ = next_slot;
        entry_found = true;
      } else {  // otherwise go to next bucket
        PageNum next_bucket = curr_bucket_header_->next_bucket;
        s = pfh_.UnpinPage(curr_bucket_num_); if (!s.ok()) return s;
        bucket_open_ = false;
        if (next_bucket != -1) {  // if there is a bucket after this bucket
          Page p;
          s = pfh_.GetPage(next_bucket, p); if (!s.ok()) return s;
          curr_bucket_num_ = next_bucket;
          curr_bucket_header_ = (BucketHeader *) p.data;
          bucket_slot_num_ = curr_bucket_header_->first_slot;
          bucket_entry_ = (BucketEntry *) ((char *) curr_bucket_header_
              + header_.entry_offset_b);
          bucket_open_ = true;
          entry_found = true;
        }
      }
    }
    if (!entry_found) {  // go to next entry in leaf
      int next_slot = leaf_entry_[leaf_slot_num_].next_slot;
      if (next_slot != -1) {  // next slot is in this leaf
        leaf_slot_num_ = next_slot;
        if (leaf_entry_[leaf_slot_num_].is_duplicate)
          OpenFirstBucket();
      } else {  // next slot is in next leaf
        PageNum next_leaf = curr_leaf_header_->next_page;
        s = pfh_.UnpinPage(curr_leaf_num_); if (!s.ok()) return s;
        if (next_leaf == -1) {  // if scan has come to end
          leaf_open_ = false;
          eof = true;
        } else {  // go to first entry in next leaf
          Page p;
          curr_leaf_num_ = next_leaf;
          s = pfh_.GetPage(curr_leaf_num_, p); if (!s.ok()) return s;
          curr_leaf_header_ = (NodeHeader_L *)p.data;
          leaf_entry_ = (NodeEntry *) ((char *) curr_leaf_header_
              + header_.entry_offset_n);
          key_ = (char *) curr_leaf_header_ + header_.key_offset_n;
          leaf_slot_num_ = curr_leaf_header_->first_slot;
          OpenFirstBucket();
        }
      }
    }
  }

  value = (void *)((char *)key_ + header_.attr_length * leaf_slot_num_);
  if (bucket_open_) {
    rid.page_num = bucket_entry_[bucket_slot_num_].page;
    rid.slot_num = bucket_entry_[bucket_slot_num_].slot;
  } else {
    rid.page_num = leaf_entry_[leaf_slot_num_].page;
    rid.slot_num = leaf_entry_[leaf_slot_num_].slot;
  }
  first_entry_ = false;
  return Status::OK();
}

Status IX_IndexHandle::CloseScan() {
  Status s;
  if (!scan_start_) {
    return Status(ErrorCode::kIX, "can not close scan. scan has not started");
  }
  if (leaf_open_) {
    if (bucket_open_) {
      s = pfh_.UnpinPage(curr_bucket_num_); if (!s.ok()) return s;
    }
    s = pfh_.UnpinPage(curr_leaf_num_); if (!s.ok()) return s;
  }
  scan_start_ = false;
  return Status::OK();
}

Status IX_IndexHandle::DeleteFromBucketList(PageNum first_bucket,
                                            const RID &rid, bool &is_last_rid,
                                            RID &last_rid, PageNum &new_first_bucket,
                                            bool &new_bucket) {
  Status s;
  PageNum prev_bucket = -1;
  PageNum curr_bucket = first_bucket;
  Page p;
  is_last_rid = false;
  new_first_bucket = first_bucket;
  new_bucket = false;
  bool removed = false;
  while (curr_bucket != -1) {
    s = pfh_.GetPage(curr_bucket, p); if (!s.ok()) return s;
    BucketHeader *header = (BucketHeader *)p.data;
    BucketEntry *entry = (BucketEntry *) ((char *) header + header_.entry_offset_b);
    int prev_slot = -1;
    int curr_slot = header->first_slot;
    while (curr_slot != -1) {
      // if entry is found
      if (entry[curr_slot].page == rid.page_num &&
          entry[curr_slot].slot == rid.slot_num) {
         removed = true;
         // delete the entry
         if (prev_slot != -1) {
           entry[prev_slot].next_slot = entry[curr_slot].next_slot;
         } else {
           header->first_slot = entry[curr_slot].next_slot;
         }
         entry[curr_slot].next_slot = header->free_slot;
         header->free_slot = curr_slot;
         header->num_keys--;
         break;
      }
      prev_slot = curr_slot;
      curr_slot = entry[curr_slot].next_slot;
    }
    // if entry is removed
    if (removed) {
      PageNum next_bucket = header->next_bucket;
      s = pfh_.MarkDirty(curr_bucket); if (!s.ok()) return s;
      s = pfh_.UnpinPage(curr_bucket); if (!s.ok()) return s;
      if (header->num_keys > 1) {  // if bucket still has more than one entry, return
        return Status::OK();
      } else if (header->num_keys == 0) {  //if bucket is empty, dispose page
        // if prev bucket exists, update the pointer to next bucket
        if (prev_bucket != -1) {
          Page prev_p;
          s = pfh_.GetPage(prev_bucket,prev_p);
          BucketHeader *prev_header = (BucketHeader *) p.data;
          prev_header->next_bucket = next_bucket;
          s = pfh_.MarkDirty(prev_bucket); if (!s.ok()) return s;
          s = pfh_.UnpinPage(prev_bucket); if (!s.ok()) return s;
        } else {  // if fisrt page is empty, set new fisrt bucket
          new_first_bucket = next_bucket;
          new_bucket = true;
        }
        s = pfh_.DisposePage(curr_bucket); if (!s.ok()) return s;
      }
      break;
    }
    // if entry is not found, go to next page
    prev_bucket = curr_bucket;
    curr_bucket = header->next_bucket;
    s = pfh_.UnpinPage(curr_bucket); if (!s.ok()) return s;
  }

  if (!removed) {
    return Status(ErrorCode::kIX, "can not find the rid in bucket list");
  }

  s = pfh_.GetPage(new_first_bucket, p); if (!s.ok()) return s;
  BucketHeader *header = (BucketHeader *) p.data;
  BucketEntry *entry = (BucketEntry *) ((char *) header + header_.entry_offset_b);
  // if bucket list only has one entry, destroy bucket list, return the entry
  if (header->next_bucket == -1 && header->num_keys == 1) {
    last_rid.page_num = entry[header->first_slot].page;
    last_rid.slot_num = entry[header->first_slot].slot;
    is_last_rid = true;
    s = pfh_.UnpinPage(new_first_bucket); if (!s.ok()) return s;
    s = pfh_.DisposePage(new_first_bucket); if (!s.ok()) return s;
  } else {
    s = pfh_.UnpinPage(new_first_bucket); if (!s.ok()) return s;
  }
  return Status::OK();
}

void IX_IndexHandle::FindIndexInNode(NodeHeader *node_header,
                                     void *value, int &index,
                                     int &prev_index, bool &is_dup) {
  int prev_idx = -1;
  prev_index = -1;
  int curr_idx = node_header->first_slot;
  NodeEntry *entry = (NodeEntry *) ((char *)node_header + header_.entry_offset_n);
  char *key = (char *) node_header + header_.key_offset_n;
  is_dup = false;
  while (curr_idx != -1) {
    char *data = key + curr_idx * header_.attr_length;
    int comp = comparator_(value, (void *) data, header_.attr_length);
    if (comp < 0) {
      break;
    } else {
      if (comp == 0) is_dup = true;
      prev_index = prev_idx;
    }
    prev_idx = curr_idx;
    curr_idx = entry[curr_idx].next_slot;
  }
  index = prev_idx;
  return;
}

Status IX_IndexHandle::SplitNode(NodeHeader *parent_header, NodeHeader *header,
                                 PageNum page, int index, int &new_index,
                                 PageNum &new_page) {
  Status s;
  Page new_p;
  s = CreateNode(new_page); if (!s.ok()) return s;
  s = pfh_.GetPage(new_page, new_p); if(!s.ok()) return s;
  NodeHeader *new_header = (NodeHeader *) new_p.data;

  NodeEntry *entry = (NodeEntry *) ((char *) header + header_.entry_offset_n);
  NodeEntry *new_entry = (NodeEntry *) ((char *) new_header + header_.entry_offset_n);
  NodeEntry *parent_entry = (NodeEntry *) ((char *) parent_header + header_.entry_offset_n);
  char *key = (char *) header + header_.key_offset_n;
  char *new_key = (char *) new_header + header_.key_offset_n;
  char *parent_key = (char *) parent_header + header_.key_offset_n;

  int prev_index = -1;
  int curr_index = header->first_slot;
  for (int i = 0 ; i < header_.max_keys_n / 2; i++) {
    prev_index = curr_index;
    curr_index = entry[curr_index].next_slot;
  }
  entry[prev_index].next_slot = -1;

  // insert middle entry into parent node
  new_index = parent_header->free_slot;
  parent_header->free_slot = parent_entry[new_index].next_slot;
  parent_entry[new_index].is_duplicate = false;
  parent_entry[new_index].page = new_page;
  if (index == -1) {
    parent_entry[new_index].next_slot = parent_header->first_slot;
    parent_header->first_slot = new_index;
  } else {
    parent_entry[new_index].next_slot = parent_entry[index].next_slot;
    parent_entry[index].next_slot = new_index;
  }
  memcpy(parent_key + new_index * header_.attr_length,
         key + curr_index * header_.attr_length, header_.attr_length);
  parent_header->num_keys++;

  if (!header->is_leaf) {  // if it is internal node, update first page
    NodeHeader_I *new_header_I = (NodeHeader_I *) new_header;
    new_header_I->first_page = entry[curr_index].page;
    new_header_I->is_leaf = false;
    prev_index = curr_index;
    curr_index = entry[curr_index].next_slot;
    entry[prev_index].next_slot = header->free_slot;
    header->free_slot = prev_index;
    header->num_keys--;
  } else {  // if it is leaf node, update prev and next page
    NodeHeader_L *new_header_L = (NodeHeader_L *) new_header;
    NodeHeader_L *header_L = (NodeHeader_L *) header;
    new_header_L->is_leaf = true;
    new_header_L->next_page = header_L->next_page;
    new_header_L->prev_page = page;
    header_L->next_page = new_page;
    PageNum next_page = new_header_L->next_page;
    if (next_page != -1) {
      Page next_p;
      s = pfh_.GetPage(next_page, next_p); if (!s.ok()) return s;
      NodeHeader_L *next_header_L = (NodeHeader_L *) next_p.data;
      next_header_L->prev_page = new_page;
      s = pfh_.MarkDirty(next_page); if (!s.ok()) return s;
      s = pfh_.UnpinPage(next_page); if (!s.ok()) return s;
    }
  }

  // transfer the remaining entry
  int new_prev_index = -1;
  int new_curr_index = new_header->free_slot;
  while (curr_index != -1) {
    new_header->free_slot = new_entry[new_curr_index].next_slot;
    new_entry[new_curr_index].page = entry[curr_index].page;
    new_entry[new_curr_index].slot = entry[curr_index].slot;
    new_entry[new_curr_index].is_duplicate = entry[curr_index].is_duplicate;
    memcpy((char *) new_key + new_curr_index * header_.attr_length,
           (char *) key + curr_index * header_.attr_length, header_.attr_length);
    if (new_prev_index == -1) {
      new_header->first_slot = new_curr_index;
    } else {
      new_entry[new_prev_index].next_slot = new_curr_index;
    }
    new_prev_index = new_curr_index;
    new_curr_index = new_header->free_slot;

    prev_index = curr_index;
    curr_index = entry[curr_index].next_slot;
    entry[prev_index].next_slot = header->free_slot;
    header->free_slot = prev_index;
    header->num_keys--;
    new_header->num_keys++;
  }
  new_entry[new_prev_index].next_slot = -1;
  s = pfh_.MarkDirty(new_page); if (!s.ok()) return s;
  s = pfh_.UnpinPage(new_page); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_IndexHandle::CreateNode(PageNum &page_num) {
  Status s;
  Page p;
  s = pfh_.AllocatePage(p); if (!s.ok()) return s;
  NodeHeader *header = (NodeHeader *) p.data;
  page_num = p.num;
  header->first_slot = -1;
  header->free_slot = 0;
  header->num_keys = 0;
  NodeEntry *entry = (NodeEntry *) ((char *)header + header_.entry_offset_n);
  for (int i = 0; i < header_.max_keys_n; i++) {
    entry[i].next_slot = i + 1;
  }
  entry[header_.max_keys_n - 1].next_slot = -1;
  s = pfh_.MarkDirty(p.num); if (!s.ok()) return s;
  s = pfh_.UnpinPage(p.num); if (!s.ok()) return s;
  return Status::OK();
}

Status IX_IndexHandle::CreateBucket(PageNum &page_num) {
  Status s;
  Page p;
  s = pfh_.AllocatePage(p); if (!s.ok()) return s;
  BucketHeader *header = (BucketHeader *) p.data;
  header->first_slot = -1;
  header->free_slot = 0;
  header->next_bucket = -1;
  header->num_keys = 0;
  BucketEntry *entry = (BucketEntry *) ((char *) header + header_.entry_offset_b);
  for (int i = 0; i < header_.max_keys_b; i++) {
     entry[i].next_slot = i + 1;
  }
  entry[header_.max_keys_b - 1].next_slot = -1;
  s = pfh_.MarkDirty(p.num); if (!s.ok()) return s;
  s = pfh_.UnpinPage(p.num); if (!s.ok()) return s;
  page_num = p.num;
  return Status::OK();
}

Status IX_IndexHandle::InsertIntoBucket(PageNum page_num, const RID &rid) {
  Status s;
  Page p;
  PageNum curr_page = page_num;
  while(true) {
    s = pfh_.GetPage(curr_page, p); if (!s.ok()) return s;
    BucketHeader *header = (BucketHeader *) p.data;
    BucketEntry *entry = (BucketEntry *)((char *)header + header_.entry_offset_b);
    int curr_index = header->first_slot;
    while(curr_index != -1) {
      if (entry[curr_index].page == rid.page_num
          && entry[curr_index].slot == rid.slot_num) {
        s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
        return Status(ErrorCode::kIX,
                      "can not insert into bucket.duplicate entries in bucket");
      }
      curr_index = entry[curr_index].next_slot;
    }
    if (header->next_bucket == -1) {  // if it is the last page
      if (header->free_slot == -1) {  // if it is full, create new bucket
        PageNum new_bucket;
        Page new_p;
        s = CreateBucket(new_bucket); if (!s.ok()) return s;
        header->next_bucket = new_bucket;
        s = pfh_.MarkDirty(curr_page); if (!s.ok()) return s;
        s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
        curr_page = new_bucket;
        s = pfh_.GetPage(curr_page, new_p); if (!s.ok()) return s;
        header = (BucketHeader *) new_p.data;
        entry = (BucketEntry *)((char *)header + header_.entry_offset_b);
      }
      // insert the rid into bucket
      int slot = header->free_slot;
      header->free_slot = entry[slot].next_slot;
      entry[slot].next_slot = header->first_slot;
      header->first_slot = slot;
      entry[slot].page = rid.page_num;
      entry[slot].slot = rid.slot_num;
      header->num_keys++;
      s = pfh_.MarkDirty(curr_page); if (!s.ok()) return s;
      s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
      break;
    } else { // it is not the last page
      PageNum next_page = header->next_bucket;
      s = pfh_.UnpinPage(curr_page); if (!s.ok()) return s;
      curr_page = next_page;
    }
  }
  return Status::OK();
}

}  // namespace toydb
