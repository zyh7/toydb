/*
 * rm_manager.cc
 *
 *  Created on: Apr 24, 2018
 *      Author: zyh
 */

#include "rm.h"
#include "pf_internal.h"
#include "status.h"
#include <math.h>
#include <string.h>

namespace toydb {

RM_Manager::RM_Manager(PF_Manager &pfm) : pf_manager_(pfm){

}

RM_Manager::~RM_Manager() {
}

int RM_Manager::CalcNumRecordsPerPage(int record_size) {
  if (record_size <= 0) return 0;
  return floor((kPageSize - sizeof(PF_PageHeader))*1.0 /
               (1.0 * record_size + 1.0 / 8));
}

int RM_Manager::CalcBitmapSize(int num_records) {
  return ceil(num_records / 8.0);
}



Status RM_Manager::CreateFile(const char *file_name, int record_size) {
  Status s;
  if (file_name == nullptr) {
    return Status(ErrorCode::kRM, "file name is null");
  }
  int num_records_per_page = CalcNumRecordsPerPage(record_size);
  if (num_records_per_page <= 0) {
    return Status(ErrorCode::kRM, "can not create file.Invalid record size");
  }
  s = pf_manager_.CreateFile(file_name); if (!s.ok()) return s;
  PF_FileHandle fh;
  Page p;
  RM_FileHeader *header;
  s = pf_manager_.OpenFile(file_name, fh); if (!s.ok()) return s;
  s = fh.AllocatePage(p); if (!s.ok()) return s;
  header = (RM_FileHeader *) p.data;
  header->next_free_page = -1;
  header->num_pages = 0;
  header->bitmap_offset = sizeof(RM_PageHeader);
  header->num_record_per_page = num_records_per_page;
  header->record_size = record_size;
  header->bitmap_size = CalcBitmapSize(num_records_per_page);
  s = fh.MarkDirty(0); if (!s.ok()) return s;
  s = fh.UnpinPage(0); if (!s.ok()) return s;
  s = pf_manager_.CloseFile(fh); if (!s.ok()) return s;
  return Status::OK();
}

Status RM_Manager::OpenFile(const char *file_name, RM_FileHandle &file_handle) {
  if (file_name == nullptr) {
    return Status(ErrorCode::kRM, "can not open file. file name is null");
  }
  Status s;
  Page p;
  s = pf_manager_.OpenFile(file_name, file_handle.pfh_);
  s = file_handle.pfh_.GetPage(0, p); if (!s.ok()) return s;
  memcpy(&file_handle.header_, p.data, sizeof(RM_FileHeader));
  file_handle.file_open_ = true;
  file_handle.header_changed_ = false;
  s = file_handle.pfh_.UnpinPage(0); if (!s.ok()) return s;
  return Status::OK();
}

Status RM_Manager::CloseFile(RM_FileHandle &file_handle) {
  Status s;
  if (file_handle.file_open_ == false) {
    return Status(ErrorCode::kRM, "can not close file. file is not open");
  }
  Page p;
  if (file_handle.header_changed_ == true) {
    s = file_handle.pfh_.GetPage(0, p); if (!s.ok()) return s;
    memcpy(p.data, &file_handle.header_, sizeof(RM_PageHeader));
    s = file_handle.pfh_.MarkDirty(0); if (!s.ok()) return s;
    s = file_handle.pfh_.UnpinPage(0); if (!s.ok()) return s;
  }
  s = pf_manager_.CloseFile(file_handle.pfh_); if (!s.ok()) return s;
  file_handle.file_open_ = false;
  return Status::OK();
}

Status RM_Manager::DeleteFile(const char *file_name) {
  Status s;
  if (file_name == nullptr) {
    return Status(ErrorCode::kRM, "can not delete file.file name is null");
  }
  s = pf_manager_.DeleteFile(file_name); if (!s.ok()) return s;
  return Status::OK();
}

}  // namespace toydb


