/*
 * ql_nodejoin.cc
 *
 *  Created on: Jul 3, 2018
 *      Author: zyh
 */

#include <stdlib.h>
#include "ql_node.h"

namespace toydb {

QL_NodeJoin::QL_NodeJoin(QL_Manager &qlm, QL_Node &node1, QL_Node &node2)
    : QL_Node(qlm),
      node1_(node1),
      node2_(node2) {
  use_index_join_ = false;
  buffer_ = nullptr;
}

QL_NodeJoin::~QL_NodeJoin() {
  if (node_set_) {
    free(buffer_);
    delete[] attr_list_;
    delete[] cond_list_;
  }
}

void QL_NodeJoin::SetUpNode(int num_conds) {
  int *attr_list1;
  int *attr_list2;
  int num_attrs1;
  int num_attrs2;
  node1_.GetAttrList(attr_list1, num_attrs1);
  node2_.GetAttrList(attr_list2, num_attrs2);
  num_attrs_ = num_attrs1 + num_attrs2;
  attr_list_ = new int[num_attrs_];
  for (int i = 0; i < num_attrs1; i++) {
    attr_list_[i] = attr_list1[i];
  }
  for (int i = 0; i < num_attrs2; i++) {
    attr_list_[i + num_attrs1] = attr_list2[i];
  }
  cond_list_ = new Cond[num_conds];
  int len1, len2;
  node1_.GetTupleLength(len1);
  node2_.GetTupleLength(len2);
  tuple_len_ = len1 + len2;
  first_tuple_len_ = len1;
  buffer_ = malloc(tuple_len_);
  node_set_ = true;
  return;
}

Status QL_NodeJoin::OpenIt() {
  Status s;
  if (!use_index_join_) {
    s = node1_.OpenIt(); if (!s.ok()) return s;
    s = node2_.OpenIt(); if (!s.ok()) return s;
  } else {
    s = node1_.OpenIt(); if (!s.ok()) return s;
  }
  got_first_tuple_ = false;
  return Status::OK();
}

Status QL_NodeJoin::CloseIt() {
  Status s;
  s = node1_.CloseIt(); if (!s.ok()) return s;
  s = node2_.CloseIt(); if (!s.ok()) return s;
  got_first_tuple_ = false;
  return Status::OK();
}

Status QL_NodeJoin::DeleteIt() {
  Status s;
  if (node_set_) {
    free(buffer_);
    delete[] attr_list_;
    delete[] cond_list_;
    node_set_ = false;
  }
  s = node1_.DeleteIt(); if (!s.ok()) return s;
  s = node2_.DeleteIt(); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_NodeJoin::OpenIt(void *value) {
  return Status(ErrorCode::kQL, "invalid function");
}

Status QL_NodeJoin::GetNext(void *data, bool &eof) {
  Status s;
  if (!got_first_tuple_) {
    s = node1_.GetNext(buffer_, eof); if (!s.ok()) return s;
    if (eof) return Status::OK();
    if (use_index_join_) {
      int offset;
      AttrSlotToOffset(index_attr_, offset);
      s = node2_.OpenIt((void *)((char *)buffer_ + offset)); if (!s.ok()) return s;
    }
  }
  got_first_tuple_ = true;
  while (1) {
    bool end;
    s = node2_.GetNext((void *)((char *)buffer_ + first_tuple_len_), end); if (!s.ok()) return s;
    if (end) {
      s = node1_.GetNext(buffer_, eof); if (!s.ok()) return s;
      if (eof) return Status::OK();
      s = node2_.CloseIt(); if (!s.ok()) return s;
      if (use_index_join_) {
        int offset;
        AttrSlotToOffset(index_attr_, offset);
        s = node2_.OpenIt((void *)((char *)buffer_ + offset)); if (!s.ok()) return s;
      } else {
        s = node2_.OpenIt(); if (!s.ok()) return s;
      }
    } else {
      bool is_ok;
      CheckConditions(buffer_, is_ok);
      if (is_ok) break;
    }
  }
  memcpy(data, buffer_, tuple_len_);
  return Status::OK();
}

Status QL_NodeJoin::GetNextRec(Record &rec, bool &eof) {
  return Status(ErrorCode::kQL, "invalide function :ql_nodejoin GetNextRec");
}

Status QL_NodeJoin::UseIndexJoin(int left_attr, int right_attr, int index_num) {
  use_index_join_ = true;
  index_attr_ = left_attr;
  node2_.UseIndex(right_attr, index_num, nullptr);
  return Status::OK();
}

void QL_NodeJoin::UseIndex(int attrNum, int indexNumber, void *data) {
}

}  // namespace toydb
