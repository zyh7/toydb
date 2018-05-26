/*
 * ix_indexscan.cc
 *
 *  Created on: May 22, 2018
 *      Author: zyh
 */

#include "ix.h"
#include "status.h"
#include "rm.h"
#include <stdlib.h>

namespace toydb {

bool equal(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 == *(int *) value2;
    case FLOAT: return *(float *) value1 == *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) == 0;
    default: return true;
  }
}

bool not_equal(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 != *(int *) value2;
    case FLOAT: return *(float *) value1 != *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) != 0;
    default: return true;
  }
}

bool less_than(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 < *(int *) value2;
    case FLOAT: return *(float *) value1 < *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) < 0;
    default: return true;
  }
}

bool greater_than(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 > *(int *) value2;
    case FLOAT: return *(float *) value1 > *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) > 0;
    default: return true;
  }
}

bool less_equal(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 <= *(int *) value2;
    case FLOAT: return *(float *) value1 <= *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) <= 0;
    default: return true;
  }
}


bool greater_equal(void *value1, void *value2, AttrType attr_type, int attr_len) {
  switch (attr_type) {
    case INT: return *(int *) value1 >= *(int *) value2;
    case FLOAT: return *(float *) value1 >= *(float *) value2;
    case STRING: return strncmp((char *) value1, (char *) value2, attr_len) >= 0;
    default: return true;
  }
}

IX_IndexScan::IX_IndexScan() {
  scan_open_ = false;
  scan_end_ =false;
  use_first_leaf = false;
}

IX_IndexScan::~IX_IndexScan() {
  if (scan_open_)
    free(value_);
}

Status IX_IndexScan::OpenScan(IX_IndexHandle &ixh, CompOp comp_op, void *value) {
  Status s;
  if (ixh.index_open_ == false)
    return Status(ErrorCode::kIX, "can not open scan. index handle is not open");
  ixh_ = &ixh;
  comp_op_ = comp_op;

  equal_ = false;
  switch (comp_op) {
    case EQ_OP: comparator_ = &equal; equal_ = true; break;
    case NE_OP: comparator_ = &not_equal; use_first_leaf = true; break;
    case LT_OP: comparator_ = &less_than; use_first_leaf = true; break;
    case GT_OP: comparator_ = &greater_than; equal_ = false; break;
    case LE_OP: comparator_ = &less_equal; use_first_leaf = true; break;
    case GE_OP: comparator_ = &greater_equal; equal_ = true; break;
    case NO_OP: return Status(ErrorCode::kIX, "can not open scan. no compare operator");
  }
  if (use_first_leaf) {
    s = ixh.GoToFirstEntry(); if (!s.ok()) return s;
  } else {
    s = ixh.GoToAppropriateEntry(equal_, value); if (!s.ok()) return s;
  }
  attr_type_ = ixh.header_.attr_type;
  attr_len_ = ixh.header_.attr_length;
  value_ = (void *) malloc(attr_len_);
  memcpy(value_, value, attr_len_);
  scan_open_ = true;
  scan_end_ = false;
  return Status::OK();
}

Status IX_IndexScan::CloseScan() {
  Status s;
  if (!scan_open_) {
    return Status(ErrorCode::kIX, "can not close scan. scan is not open");
  }
  if (!scan_end_) {
    s = ixh_->CloseScan(); if (!s.ok()) return s;
  }
  free(value_);
  scan_open_ = false;
  return Status::OK();
}

Status IX_IndexScan::BeginScan() {
  Status s;
  if (scan_end_ || !scan_open_) {
    return Status(ErrorCode::kIX, "can not begin scan. scan has ended or is not open");
  }
  if (use_first_leaf) {
    ixh_->GoToFirstEntry();
  } else {
    ixh_->GoToAppropriateEntry(equal_, value_);
  }
  scan_open_ = true;
  return Status::OK();
}

Status IX_IndexScan::GetNextEntry(RID &rid, bool &eof) {
  Status s;
  if (!scan_open_) {
    return Status(ErrorCode::kIX, "can not get next entry. scan is not open");
  }
  if (scan_end_) {
    eof = true;
    return Status::OK();
  }
  void *value;
  RID next_rid;
  s = ixh_->GetNextRIDAndValue(next_rid, value, eof); if (!s.ok()) return s;
  if (eof) {
    scan_end_ = true;
    ixh_->CloseScan();
    return Status::OK();
  }

  if (comp_op_ == EQ_OP || comp_op_ == LT_OP || comp_op_ == LE_OP) {
    if (!comparator_(value, value_, attr_type_, attr_len_)) {// we can stop in advance stop scan if comp_op is equal, less than or less equal
      scan_end_ = true;
      eof = true;
      ixh_->CloseScan();
      return Status::OK();
    }
  } else if (comp_op_ == NE_OP) {
    // skip value that is equal
    while(!comparator_(value, value_, attr_type_, attr_len_)) {
      s = ixh_->GetNextRIDAndValue(next_rid, value, eof); if (!s.ok()) return s;
      if (eof) {
        scan_end_ = true;
        ixh_->CloseScan();
        return Status::OK();
      }
    }
  }
  // if comp == NO_OP || GT_OP || GE_OP, no need to stop or skip


  eof = false;
  rid = next_rid;
  return Status::OK();
}

}  // namespace toydb
