/*
 * ql_nodeproj.cc
 *
 *  Created on: Jul 4, 2018
 *      Author: zyh
 */

#include "ql_node.h"
#include <stdlib.h>

namespace toydb {

QL_NodeProj::QL_NodeProj(QL_Manager &qlm, QL_Node &prev_node)
    : QL_Node(qlm),
      prev_node_(prev_node) {
  buffer_ = nullptr;
  attr_keep_list_ = nullptr;
  attr_keep_offset_ = nullptr;
  attr_len_ = nullptr;
  num_attrs_keep_ = 0;
}

QL_NodeProj::~QL_NodeProj() {
  if (node_set_) {
    delete[] attr_keep_list_;
    delete[] attr_keep_offset_;
    delete[] attr_len_;
    free(buffer_);
  }
}

void QL_NodeProj::SetUpNode(int num_attrs_keep, int *attr_keep_list) {
  // set up attr list and buffer
  prev_node_.GetTupleLength(tuple_len_);
  buffer_ = malloc(tuple_len_);
  prev_node_.GetAttrList(attr_list_, num_attrs_);

  num_attrs_keep_ = num_attrs_keep;
  attr_keep_list_ = new int[num_attrs_keep];
  attr_keep_offset_ = new int[num_attrs_keep];
  attr_len_ = new int [num_attrs_keep];
  for (int i = 0; i < num_attrs_keep_; i++) {
    attr_keep_list_[i] = attr_keep_list[i];
    int slot = attr_keep_list_[i];
    AttrSlotToOffset(slot, attr_keep_offset_[i]);
    attr_len_[i] = qlm_.attr_entry_[i].attrLength;
  }
  node_set_ = true;
  return;
}

Status QL_NodeProj::OpenIt() {
  Status s;
  s = prev_node_.OpenIt(); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeProj::OpenIt(void *value) {
  return Status(ErrorCode::kQL, "invalid function");
}

Status QL_NodeProj::CloseIt() {
  Status s;
  s = prev_node_.CloseIt(); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeProj::DeleteIt() {
  Status s;
  if (node_set_) {
    delete[] attr_keep_list_;
    delete[] attr_keep_offset_;
    delete[] attr_len_;
    free(buffer_);
    node_set_ = false;
  }
  s = prev_node_.DeleteIt(); if (!s.ok()) return s;
  return s;
}

Status QL_NodeProj::GetNext(void *data, bool &eof) {
  Status s;
  s = prev_node_.GetNext(buffer_, eof); if (!s.ok()) return s;
  if (eof) return Status::OK();
  int len = 0;
  for (int i = 0; i < num_attrs_keep_; i++) {
    memcpy((char *)data + len, (char *)buffer_ + attr_keep_offset_[i], attr_len_[i]);
    len += attr_len_[i];
  }
  return Status::OK();
}

void QL_NodeProj::UseIndex(int attrNum, int indexNumber, void *data) {

}

Status QL_NodeProj::GetNextRec(Record &rec, bool &eof) {
  return Status(ErrorCode::kQL, "invalid function");
}

void QL_NodeProj::GetAttrList(int *&attr_list, int &num_attrs) {
  attr_list = attr_keep_list_;
  num_attrs = num_attrs_keep_;
  return;
}

}  // namespace toydb
