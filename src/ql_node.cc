/*
 * ql_node.cc
 *
 *  Created on: Jun 8, 2018
 *      Author: zyh
 */

#include "ql_node.h"

namespace toydb {

extern bool equal(void *value1, void *value2, AttrType attr_type, int attr_len);

extern bool not_equal(void *value1, void *value2, AttrType attr_type, int attr_len);

extern bool less_than(void *value1, void *value2, AttrType attr_type, int attr_len);

extern bool greater_than(void *value1, void *value2, AttrType attr_type, int attr_len);

extern bool less_equal(void *value1, void *value2, AttrType attr_type, int attr_len);

extern bool greater_equal(void *value1, void *value2, AttrType attr_type, int attr_len);

QL_Node::QL_Node(QL_Manager &qlm) : qlm_(qlm) {
  attr_list_ = nullptr;
  cond_list_ = nullptr;
  node_set_ = false;
  tuple_len_ = 0;
  num_attrs_ = 0;
  cond_num_ = 0;
}

QL_Node::~QL_Node() {

}

void QL_Node::GetTupleLength(int &length) {
  length = tuple_len_;
  return;
}

void QL_Node::GetAttrList(int *&attr_list, int &num_attrs) {
  attr_list = attr_list_;
  num_attrs = num_attrs_;
  return;
}

Status QL_Node::AddConditions(const Condition &cond) {
  Status s;
  int slot;
  int offset;

  s = qlm_.GetAttrEntryPos(cond.l_attr, slot); if (!s.ok()) return s;
  s = AttrSlotToOffset(slot, offset); if (!s.ok()) return s;
  cond_list_[cond_num_].attr_type = qlm_.attr_entry_[slot].attrType;
  cond_list_[cond_num_].length = qlm_.attr_entry_[slot].attrLength;
  cond_list_[cond_num_].offset1 = offset;

  if (cond.r_is_attr) {
    s = qlm_.GetAttrEntryPos(cond.r_attr, slot); if (!s.ok()) return s;
    s = AttrSlotToOffset(slot, offset); if (!s.ok()) return s;
    cond_list_[cond_num_].offset2 = offset;
    cond_list_[cond_num_].is_attr = true;
  } else {
    cond_list_[cond_num_].value = cond.r_value.data;
    cond_list_[cond_num_].is_attr = false;
  }

  switch(cond.op) {
    case EQ_OP: cond_list_[cond_num_].comparator_ = &equal; break;
    case NE_OP: cond_list_[cond_num_].comparator_ = &not_equal; break;
    case LT_OP: cond_list_[cond_num_].comparator_ = &less_than; break;
    case GT_OP: cond_list_[cond_num_].comparator_ = &greater_than; break;
    case LE_OP: cond_list_[cond_num_].comparator_ = &less_than; break;
    case GE_OP: cond_list_[cond_num_].comparator_ = &greater_equal; break;
    default:
      return Status(ErrorCode::kQL, "unknown type");
  }
  cond_num_++;
  return Status::OK();
}

void QL_Node::CheckConditions(void *buffer,bool &is_ok) {
  Status s;
  for (int i = 0; i < cond_num_; i++) {
    void *value1 = (void *)((char *) buffer + cond_list_[i].offset1);
    if (cond_list_[i].is_attr) {
      void *value2 = (void *)((char *) buffer + cond_list_[i].offset2);
      bool comp = cond_list_[i].comparator_(value1, value2,
                                            cond_list_[i].attr_type,
                                            cond_list_->length);
      if (comp == false) {
        is_ok = false;
        return;
      }
    } else {
      void *value2 = cond_list_[i].value;
      bool comp = cond_list_[i].comparator_(value1, value2,
                                            cond_list_[i].attr_type,
                                            cond_list_->length);
      if (comp == false) {
        is_ok = false;
        return;
      }
    }
  }
  is_ok = true;
  return;
}

Status QL_Node::AttrSlotToOffset(int slot, int &offset) {
  int length = 0;
  for (int i = 0; i < num_attrs_; i++) {
    int temp_slot = attr_list_[i];
    if (temp_slot == slot) {
      offset = length;
      return Status::OK();
    }
    length += qlm_.attr_entry_[temp_slot].attrLength;
  }
  return Status(ErrorCode::kQL, "AttrSlotToOffset error");
}

}  // namespace toydb
