/*
 * rm_filescan.cc
 *
 *  Created on: Apr 28, 2018
 *      Author: zyh
 */

#include "rm.h"
#include <string.h>
#include <stdlib.h>


namespace toydb {

RM_FileScan::RM_FileScan() {
  open_scan_ = false;
  value_ = nullptr;
}

RM_FileScan::~RM_FileScan() {
  open_scan_ = false;
  if (value_ != nullptr) {
    free(value_);
  }
}

bool equal(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 == *(int *)value2);
    case FLOAT: return (*(float *)value1 == *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) == 0;
    default: return true;
  }
}

bool not_equal(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 != *(int *)value2);
    case FLOAT: return (*(float *)value1 != *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) != 0;
    default: return true;
  }
}

bool less_than(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 < *(int *)value2);
    case FLOAT: return (*(float *)value1 < *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) < 0;
    default: return true;
  }
}

bool greater_than(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 > *(int *)value2);
    case FLOAT: return (*(float *)value1 > *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) > 0;
    default: return true;
  }
}

bool less_equal(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 <= *(int *)value2);
    case FLOAT: return (*(float *)value1 <= *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) <= 0;
    default: return true;
  }
}

bool greater_equal(void *value1, void *value2, int size, AttrType type) {
  switch (type) {
    case INT: return (*(int *)value1 >= *(int *)value2);
    case FLOAT: return (*(float *)value1 >= *(float *)value2);
    case STRING: return strncmp((char *)value1, (char *)value2, size) >= 0;
    default: return true;
  }
}

Status RM_FileScan::OpenScan(RM_FileHandle &fh, AttrType attr_type,
                             int attr_length, int attr_offset, CompOp comp_op,
                             void *value) {
  if (fh.file_open_ == false) {
    return Status(ErrorCode::kRM, "file handle has not opened file");
  }
  if (attr_length < 0) {
    return Status(ErrorCode::kRM, "attr_length is negative");
  }
  if (attr_type == FLOAT || attr_type == INT) {
    if (attr_length  != 4) {
      return Status(ErrorCode::kRM, "attr_length error");
    }
  }
  if (attr_offset + attr_length > fh.header_.record_size) {
    return Status(ErrorCode::kRM, "attribute location is out of record range");
  }
  fh_ = &fh;
  attr_type_ = attr_type;
  attr_length_ = attr_length;
  attr_offset_ = attr_offset;
  comp_op_ = comp_op;

  switch (comp_op) {
    case EQ_OP: comparator_ = &equal; break;
    case NE_OP: comparator_ = &not_equal; break;
    case LT_OP: comparator_ = &less_than; break;
    case GT_OP: comparator_ = &greater_than; break;
    case LE_OP: comparator_ = &less_equal; break;
    case GE_OP: comparator_ = &greater_equal; break;
    case NO_OP: comparator_ = nullptr; break;
    default: return Status(ErrorCode::kRM, "invalid compare type");
  }
  if (comp_op != NO_OP) {
    value_ = malloc(attr_length);
    memcpy(value_,value,attr_length);
  }
  rid_.page_num = 1;
  rid_.slot_num = 0;
  open_scan_ = true;
  scan_end_ = false;
  return Status::OK();
}

Status RM_FileScan::GetNextRec(Record &rec, bool &eof) {
  Status s;
  if (open_scan_ == false) {
    return Status(ErrorCode::kRM, "scan is not open");
  }
  if (scan_end_ == true) {
    return Status(ErrorCode::kRM, "scan has ended");
  }
  while (1) {
    s = fh_->GetNextRec(rid_, rec, eof); if (!s.ok()) return s;
    if (eof == true) {
      scan_end_ = true;
      break;
    }
    if (rid_.slot_num == fh_->header_.num_record_per_page - 1) {
      rid_.page_num++;
      rid_.slot_num = 0;
    } else {
      rid_.slot_num++;
    }
    char *value = rec.data_ + attr_offset_;
    if (comp_op_ == NO_OP || comparator_((void *)value, value_, attr_length_, attr_type_)) {
      break;
    }

  }
  return Status::OK();
}

Status RM_FileScan::CloseScan() {
  if (open_scan_ == false) {
    return Status(ErrorCode::kRM, "can not close scan. scan is not open");
  }
  if (value_ != nullptr) {
    free(value_);
    value_ = nullptr;
  }
  open_scan_ = false;
  return Status::OK();
}

}


