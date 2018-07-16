/*
 * ql_nodosel.cc
 *
 *  Created on: Jun 7, 2018
 *      Author: zyh
 */

#include <string.h>
#include <stdlib.h>
#include "ql_node.h"

namespace toydb {

QL_NodeRel::QL_NodeRel(QL_Manager &qlm, RelEntry *r_entry) : QL_Node(qlm) {
  r_entry_ = r_entry;
  use_index_ = false;
  tuple_len_ = r_entry->tupleLength;
  value_ = nullptr;
  attr_list_ = nullptr;
}

QL_NodeRel::~QL_NodeRel() {
  if (node_set_) {
    delete[] attr_list_;
  }
}

Status QL_NodeRel::OpenIt() {
  Status s;
  if (use_index_) {
    s = qlm_.ixm_.OpenIndex(r_entry_->relName, index_no_, ih_); if (!s.ok()) return s;
    s = is_.OpenScan(ih_, EQ_OP, value_); if (!s.ok()) return s;
    s = qlm_.rmm_.OpenFile(r_entry_->relName, fh_); if (!s.ok()) return s;
  } else {
    s = qlm_.rmm_.OpenFile(r_entry_->relName, fh_); if (!s.ok()) return s;
    s = fs_.OpenScan(fh_, INT, 4, 0, NO_OP, nullptr); if (!s.ok()) return s;
  }
  return Status::OK();
}

Status QL_NodeRel::CloseIt() {
   Status s;
   if (use_index_) {
     s = is_.CloseScan(); if (!s.ok()) return s;
     s = qlm_.ixm_.CloseIndex(ih_); if (!s.ok()) return s;
     s = qlm_.rmm_.CloseFile(fh_); if (!s.ok()) return s;
   } else {
     s = qlm_.rmm_.CloseFile(fh_); if (!s.ok()) return s;
     s = fs_.CloseScan(); if (!s.ok()) return s;
   }
   return Status::OK();
}

Status QL_NodeRel::OpenIt(void *value) {
  Status s;
  s = qlm_.ixm_.OpenIndex(r_entry_->relName, index_no_, ih_); if (!s.ok()) return s;
  s = is_.OpenScan(ih_, EQ_OP, value_); if (!s.ok()) return s;
  s = qlm_.rmm_.OpenFile(r_entry_->relName, fh_); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeRel::DeleteIt() {
  Status s;
  if (node_set_) {
    delete[] attr_list_;
    node_set_ = false;
  }
  return Status::OK();
}

Status QL_NodeRel::GetNext(void *data, bool &eof) {
  Status s;
  RID rid;
  Record rec;
  if (use_index_) {
    s = is_.GetNextEntry(rid, eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
    s = fh_.GetRec(rid, rec); if (!s.ok()) return s;
  } else {
    s = fs_.GetNextRec(rec,eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
  }
  memcpy(data, rec.data_, tuple_len_);
  return Status::OK();
}

Status QL_NodeRel::GetNextRec(Record &rec, bool &eof) {
  Status s;
  RID rid;
  if (use_index_) {
    s = is_.GetNextEntry(rid, eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
    s = fh_.GetRec(rid, rec); if (!s.ok()) return s;
  } else {
    s = fs_.GetNextRec(rec,eof); if (!s.ok()) return s;
  }
  return Status::OK();
}

void QL_NodeRel::SetUpAttrList(int *attr_list, int num_attrs) {
  attr_list_ = new int[num_attrs];
  num_attrs_ = num_attrs;
  for (int i = 0; i < num_attrs; i++) {
    attr_list_[i] = attr_list[i];
  }
  node_set_ = true;
  return;
}

void QL_NodeRel::UseIndex(int index_attr, int index_no, void *data) {
  index_attr_ = index_attr;
  index_no_ = index_no;
  value_ = data;
  use_index_ = true;
  return;
}

}  // namesoace toydb


