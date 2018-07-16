/*
 * ql_nodesel.cc
 *
 *  Created on: Jun 8, 2018
 *      Author: zyh
 */

#include <stdlib.h>
#include "ql_node.h"

namespace toydb {

QL_NodeSel::QL_NodeSel(QL_Manager &qlm, QL_Node &prev_node)
    : QL_Node(qlm),
      prev_node_(prev_node) {
  buffer_ = nullptr;
}

QL_NodeSel::~QL_NodeSel() {
  if (node_set_) {
    free(buffer_);
    delete[] cond_list_;
    delete[] attr_list_;
  }
}

Status QL_NodeSel::OpenIt() {
  Status s;
  s = prev_node_.OpenIt(); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeSel::OpenIt(void *value) {
  return Status(ErrorCode::kQL, "invalid function");
}

Status QL_NodeSel::DeleteIt() {
  Status s;
  if (node_set_) {
    free(buffer_);
    delete[] cond_list_;
    delete[] attr_list_;
    node_set_ = false;
  }
  s = prev_node_.DeleteIt(); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeSel::CloseIt() {
  Status s;
  s = prev_node_.CloseIt(); if (!s.ok()) return s;
  return Status::OK();
}

void QL_NodeSel::SetUpNode(int num_conds) {
  prev_node_.GetTupleLength(tuple_len_);
  buffer_ = malloc(tuple_len_);
  cond_list_ = new Cond[num_conds];
  int *attr_list;
  prev_node_.GetAttrList(attr_list, num_attrs_);
  attr_list_ = new int[num_attrs_];
  memcpy(attr_list_, attr_list, num_attrs_ * sizeof(int));
  node_set_ = true;
}


Status QL_NodeSel::GetNext(void *data, bool &eof) {
  Status s;
  while (1) {
    s = prev_node_.GetNext(buffer_, eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
    bool is_ok;
    CheckConditions(buffer_, is_ok);
    if (is_ok) break;
  }
  memcpy(data, buffer_, tuple_len_);
  return Status::OK();
}

void QL_NodeSel::UseIndex(int attrNum, int indexNumber, void *data) {
}

Status QL_NodeSel::GetNextRec(Record &rec, bool &eof) {
  Status s;
  while (1) {
    s = prev_node_.GetNextRec(rec, eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
    bool is_ok;
    CheckConditions(rec.data_, is_ok);
    if (is_ok) break;
  }
  return Status::OK();
}

}
