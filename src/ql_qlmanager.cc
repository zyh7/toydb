/*
 * ql_qlmanager.cc
 *
 *  Created on: Jun 4, 2018
 *      Author: zyh
 */

#include <string.h>
#include <stdlib.h>
#include <vector>
#include <string.h>
#include "ql_node.h"
#include "toydb.h"
#include "ql.h"

namespace toydb {

QL_Manager::QL_Manager(SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm)
    : smm_(smm),
      ixm_(ixm),
      rmm_(rmm) {

}

QL_Manager::~QL_Manager() {

}

void QL_Manager::Reset() {
  rel_to_int_.clear();
  rel_to_first_attr_.clear();
  attr_to_rel_.clear();
  rel_to_conds.clear();
  num_rels_ = 0;
  num_attrs_ = 0;
  num_conds_ = 0;
}

Status QL_Manager::Select(int num_sel_attrs, const RelAttr sel_attrs[],
                          int num_relations, const char * const relations[],
                          int num_conditions, const Condition conditions[]) {
  Status s;
  Reset();
  num_rels_ = num_relations;
  num_conds_ = num_conditions;
  conditions_ = conditions;

  rel_entry_ = new RelEntry[num_relations];
  for (int i = 0; i < num_relations; i++) {
    Record rec;
    RelEntry *r_entry;
    s = smm_.GetRelEntry(relations[i], rec, r_entry);
    if (!s.ok()) {
      delete[] rel_entry_;
      return s;
    }
    memcpy((void *) (rel_entry_ + i), (void *) r_entry, sizeof(RelEntry));
    num_attrs_ += rel_entry_[i].attrCount;
    std::string rel_string(rel_entry_[i].relName);
    rel_to_int_.insert( { rel_string, i });
  }

  attr_entry_ = new AttrEntry[num_attrs_];
  int slot = 0;
  for (int i = 0; i < num_rels_; i++) {
    s = smm_.GetAttrForRel(rel_entry_ + i, attr_entry_ + slot, attr_to_rel_);
    if (!s.ok()) {
      delete[] attr_entry_;
      return s;
    }
    std::string rel_string(rel_entry_[i].relName);
    rel_to_first_attr_.insert( {rel_string , slot });
    slot += rel_entry_[i].attrCount;
  }

  s = CheckSeletAttrs(num_sel_attrs, sel_attrs); if (!s.ok()) return s;
  s = CheckConditions(num_conditions, conditions); if (!s.ok()) return s;

  QL_Node *top_node;
  s = SetUpNodes(top_node, num_sel_attrs, sel_attrs); if (!s.ok()) return s;
  s = RunSelect(top_node); if (!s.ok()) return s;

  s = CleanUpNodes(top_node); if (!s.ok()) return s;
  delete[] rel_entry_;
  delete[] attr_entry_;
  return Status::OK();
}

Status QL_Manager::RunSelect(QL_Node *top_node) {
  Status s;
  int len;
  top_node->GetTupleLength(len);
  void *buffer = malloc(len);

  int *attr_list;
  int num_attrs;
  top_node->GetAttrList(attr_list, num_attrs);

  bool eof;
  s = top_node->OpenIt(); if (!s.ok()) return s;
  while (1) {
    top_node->GetNext(buffer, eof); if (!s.ok()) return s;
    if (eof) break;
    PrintBuffer(std::cout, buffer, attr_list, num_attrs);
  }
  free(buffer);
  s = top_node->CloseIt();
  return Status::OK();
}

void QL_Manager::PrintBuffer(std::ostream &c, void *buffer, int *attr_list, int num_attrs) {
  char str[kMaxOutputString + 1];
  int a;
  float b;
  int offset = 0;
  for (int i = 0; i < num_attrs; i++) {
    int slot = attr_list[i];
    if (attr_entry_[slot].attrType == STRING) {
      memset(str, 0, kMaxOutputString);
      if (attr_entry_[slot].attrLength > kMaxOutputString) {
        strncpy(str, (char *)buffer + offset, kMaxOutputString);
        str[kMaxOutputString - 1] = '.';
        str[kMaxOutputString - 2] = '.';
      } else {
        strncpy(str, (char *)buffer + offset, attr_entry_[slot].attrLength);
      }
      c << str;
      c << "  ";
      offset += attr_entry_[slot].attrLength;
    } else if (attr_entry_[slot].attrType == INT) {
      memcpy(&a, ((char *)buffer + offset), sizeof(int));
      c << a;
      offset += sizeof(int);
    } else if (attr_entry_[slot].attrType == FLOAT) {
      memcpy(&b, (char *)buffer + offset, sizeof(float));
      c << b;
      offset += sizeof(float);
    }
  }
  c << '\n';
  c.flush();
  return;
}

Status QL_Manager::Insert(const char *rel_name, int num_values,
                          const Value values[]) {
  Status s;
  Reset();
  rel_entry_ = new RelEntry;
  s = SetUpOneRelation(rel_name); if (!s.ok()) return s;
  if (num_values != rel_entry_->attrCount) {
    delete[] rel_entry_;
    return Status(ErrorCode::kQL, "wrong num of inserted record attributes");
  }
  attr_entry_ = new AttrEntry[rel_entry_->attrCount];
  s = smm_.GetAttrForRel(rel_entry_, attr_entry_,attr_to_rel_); if (!s.ok()) return s;
  for (int i = 0; i < num_values; i++) {
    if (values[i].type != attr_entry_[i].attrType
        || (values[i].type == STRING
            && strlen((char *) values[i].data)
                > (unsigned int) attr_entry_[i].attrLength)) {
      delete rel_entry_;
      delete[] attr_entry_;
      return Status(ErrorCode::kQL, "inserted record format error");
    }
  }
  s = InsertIntoRelation(rel_name, rel_entry_->tupleLength, num_values, values);
  delete rel_entry_;
  delete[] attr_entry_;
  return Status::OK();
}

Status QL_Manager::Delete(const char *rel_name, int n_conditions,
                          const Condition conditions[]) {
  Status s;
  Reset();
  conditions_ = conditions;
  num_conds_ = n_conditions;
  rel_entry_ = new RelEntry;
  s = SetUpOneRelation(rel_name); if (!s.ok()) return s;
  attr_entry_ = new AttrEntry[rel_entry_->attrCount];
  s = smm_.GetAttrForRel(rel_entry_, attr_entry_, attr_to_rel_); if (!s.ok()) return s;
  s = CheckConditions(num_conds_, conditions_);
  if (!s.ok()) {
    delete rel_entry_;
    delete[] attr_entry_;
    return s;
  }
  QL_Node *top_node;
  s = SetUpFirstNode(top_node); if (!s.ok()) return s;
  s = RunDelete(top_node);

  delete rel_entry_;
  delete[] attr_entry_;
  return Status::OK();
}

Status QL_Manager::RunDelete(QL_Node *top_node) {
  Status s;
  int tuple_len, num_attrs;
  int *attr_list;
  top_node->GetTupleLength(tuple_len);
  top_node->GetAttrList(attr_list, num_attrs);

  RM_FileHandle fh;
  s = rmm_.OpenFile(rel_entry_->relName, fh); if (!s.ok()) return s;
  s = top_node->OpenIt();

  IX_IndexHandle *ixh = new IX_IndexHandle[num_attrs];
  // open index handle
  for (int i = 0; i < num_attrs; i++) {
    if (attr_entry_[i].indexNo != -1) {
      s = ixm_.OpenIndex(rel_entry_->relName, attr_entry_[i].indexNo, ixh[i]);
      if (!s.ok()) return s;
    }
  }

  Record rec;
  bool eof;
  while (1) {
    s = top_node->GetNextRec(rec, eof);
    if (!s.ok())
      return s;
    if (eof)
      break;
    s = fh.DeleteRec(rec.rid_);
    if (!s.ok())
      return s;
    for (int i = 0; i < num_attrs; i++) {
      int slot = attr_list[i];
      if (attr_entry_[slot].indexNo != -1) {
        s = ixh[slot].DeleteEntry(rec.data_ + attr_entry_[slot].offset,
                                  rec.rid_);
      }
    }
  }
  s = rmm_.CloseFile(fh); if (!s.ok()) return s;
  s = top_node->CloseIt();
  if (!s.ok())
    return s;
  for (int i = 0; i < num_attrs; i++) {
    s = ixm_.CloseIndex(ixh[i]);
    if (!s.ok())
      return s;
  }
  delete[] ixh;
  return Status::OK();
}

Status QL_Manager::Update(const char *rel_name, const RelAttr &upd_attr,
                          const int is_value, const RelAttr &r_attr,
                          const Value &r_value, int num_conditions,
                          const Condition conditions[]) {
  Status s;
  Reset();
  conditions_ = conditions;
  num_conds_ = num_conditions;

  rel_entry_ = new RelEntry;
  s = SetUpOneRelation(rel_name); if (!s.ok()) return s;

  attr_entry_ = new AttrEntry[rel_entry_->attrCount];
  s = smm_.GetAttrForRel(rel_entry_, attr_entry_, attr_to_rel_); if (!s.ok()) return s;
  s = CheckConditions(num_conds_, conditions_); if (!s.ok()) return s;

  QL_Node *top_node;
  s = SetUpFirstNode(top_node); if (!s.ok()) return s;
  s = RunUpdate(top_node, upd_attr, is_value, r_attr, r_value); if (!s.ok()) return s;
  delete rel_entry_;
  delete[] attr_entry_;
  return Status::OK();
}

Status QL_Manager::RunUpdate(QL_Node *top_node, const RelAttr &upd_attr,
                             const int is_value, const RelAttr &r_attr,
                             const Value &r_value) {
  Status s;
  int num_attr;
  int *attr_list;
  top_node->GetAttrList(attr_list, num_attr);

  RM_FileHandle fh;
  s = rmm_.OpenFile(rel_entry_->relName, fh); if (!s.ok()) return s;
  int slot1, slot2;
  s = GetAttrEntryPos(upd_attr, slot1); if (!s.ok()) return s;
  if (!is_value) {
    s = GetAttrEntryPos(r_attr, slot2); if (!s.ok()) return s;
  }

  IX_IndexHandle ih;
  if (attr_entry_[slot1].indexNo != -1) {
    s = ixm_.OpenIndex(rel_entry_->relName, attr_entry_[slot1].indexNo, ih);
    if (!s.ok()) return s;
  }

  s = top_node->OpenIt();
  Record rec;
  bool eof;
  while (1) {
    s = top_node->GetNextRec(rec, eof);
    if (eof) break;
    if (attr_entry_[slot1].indexNo != -1) {
      s = ih.DeleteEntry(rec.data_, rec.rid_); if (!s.ok()) return s;
    }
    if (is_value) {
      if (attr_entry_[slot1].attrType == STRING) {
        int value_len = strlen((char *) r_value.data); if (!s.ok()) return s;
        int copy_len = value_len + 1 > attr_entry_[slot1].attrLength ?
                attr_entry_[slot1].attrLength : value_len + 1;
        memcpy(rec.data_ + attr_entry_[slot1].offset, (char *) r_value.data,
               copy_len);
      } else {
        memcpy(rec.data_ + attr_entry_[slot1].offset, (char *) r_value.data,
               attr_entry_[slot1].attrLength);
      }
    } else {
      if (attr_entry_[slot2].attrLength >= attr_entry_[slot1].attrLength) {
        memcpy(rec.data_ + attr_entry_[slot1].offset,
               rec.data_ + attr_entry_[slot1].offset,
               attr_entry_[slot1].attrLength);
      } else {
        memcpy(rec.data_ + attr_entry_[slot1].offset,
               rec.data_ + attr_entry_[slot1].offset,
               attr_entry_[slot2].attrLength);
      }
    }
    s = fh.UpdateRec(rec); if (!s.ok()) return s;
    if (attr_entry_[slot1].indexNo != -1) {
      s = ih.InsertEntry(rec.data_, rec.rid_); if (!s.ok()) return s;
    }
  }
  s = top_node->CloseIt(); if (!s.ok()) return s;
  if (attr_entry_[slot1].indexNo != -1) {
    s = ixm_.CloseIndex(ih);
  }
  s = rmm_.CloseFile(fh); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_Manager::SetUpOneRelation(const char *rel_name) {
  Status s;
  RelEntry *rel_entry;
  Record rel_rec;
  s = smm_.GetRelEntry(rel_name, rel_rec,rel_entry); if (!s.ok()) return s;
  memcpy(rel_entry_, rel_entry, sizeof(RelEntry));
  num_rels_ = 1;
  num_attrs_ = rel_entry->attrCount;
  std::string rel_string(rel_name);
  rel_to_int_.insert({rel_string, 0});
  rel_to_first_attr_.insert({rel_string, 0});
  return Status::OK();
}

Status QL_Manager::CleanUpNodes(QL_Node *top_node) {
   Status s;
   s = top_node->DeleteIt(); if (!s.ok()) return s;
   delete top_node;
   return Status::OK();
}

Status QL_Manager::InsertIntoRelation(const char *rel_name, int tuple_len,
                                      int num_values, const Value values[]) {
  Status s;
  RM_FileHandle fh;
  s = rmm_.OpenFile(rel_name, fh); if (!s.ok()) return s;
  char *buf = (char *)malloc(tuple_len);
  for (int i = 0; i < num_values; i++) {
    memcpy(buf + attr_entry_[i].offset, values[i].data,
           attr_entry_[i].attrLength);
  }
  RID rid;
  s = fh.InsertRec(buf, rid); if (!s.ok()) return s;
  for (int i = 0; i < num_values; i++) {
    AttrEntry *a_entry = attr_entry_ + i;
    if (a_entry->indexNo != -1) {
      IX_IndexHandle ih;
      s = ixm_.OpenIndex(rel_name, a_entry->indexNo, ih); if (!s.ok()) return s;
      s = ih.InsertEntry(buf, rid); if (!s.ok()) return s;
      s = ixm_.CloseIndex(ih); if (!s.ok()) return s;
    }
  }
  free(buf);
  s = rmm_.CloseFile(fh); if (!s.ok()) return s;
  return Status::OK();
}

Status QL_Manager::CheckSeletAttrs(int num_attrs, const RelAttr attrs[]) {
  if (num_attrs == 1 && strcmp(attrs[0].attr_name, "*") == 0) {
    return Status::OK();
  }
  for (int i = 0; i < num_attrs; i++) {
    if (!IsValidAttr(attrs[i])) {
      return Status(ErrorCode::kQL, "select attr is invalid");
    }
  }
  return Status::OK();
}

bool QL_Manager::IsValidAttr(const RelAttr &attr) {
  if (attr.rel_name != nullptr) {
    std::string rel_string(attr.rel_name);
    if (rel_to_int_.find(rel_string) == rel_to_int_.end())
      return false;

    std::string attr_string(attr.attr_name);
    std::map<std::string, std::vector<std::string>>::iterator iter =
        attr_to_rel_.find(attr_string);
    if (iter == attr_to_rel_.end())
      return false;

    std::vector<std::string> rels = iter->second;
    for (unsigned int i = 0; i < rels.size(); i++) {
      if (rel_string == rels[i]) return true;
    }
    return false;
  } else {
    std::string attr_string(attr.attr_name);
    if (attr_to_rel_[attr_string].size() == 1) {
       return true;
    } else {
      return false;
    }
  }
}

Status QL_Manager::CheckConditions(int num_conds, const Condition conditions[]) {
  Status s;
  for (int i = 0; i < num_conds; i++) {
    // check left attr
    if (!IsValidAttr(conditions[i].l_attr))
      return Status(ErrorCode::kQL, "left attr in condition is invalid");
    AttrEntry *l_attr_entry;
    s = GetAttrEntry(conditions[i].l_attr, l_attr_entry); if (!s.ok()) return s;
    AttrType l_type = l_attr_entry->attrType;
    std::string l_rel_string(l_attr_entry->attrName);
    int l_rel_num = rel_to_int_[l_rel_string];
    int rel_num;
    // check right attr
    if (conditions[i].r_is_attr) {
      if (!IsValidAttr(conditions[i].r_attr))
        return Status(ErrorCode::kQL, "right attr in condition is invalid");;
      AttrEntry *r_attr_entry;
      s = GetAttrEntry(conditions[i].r_attr, r_attr_entry); if (!s.ok()) return s;
      AttrType r_type = r_attr_entry->attrType;
      if (r_type != l_type) {
        return Status(ErrorCode::kQL,
                      "left and right type in condition is not consistant");
      }
      std::string r_rel_string(r_attr_entry->attrName);
      int r_rel_num = rel_to_int_[r_rel_string];
      rel_num = std::max(l_rel_num, r_rel_num);

    } else {
      if (conditions[i].r_value.type != l_type){
        return Status(ErrorCode::kQL,
                      "left and right type in condition is not consistant");
      }
      rel_num = l_rel_num;
    }

    std::map <int, std::vector <int>>::iterator it = rel_to_conds.find(rel_num);
    if (it == rel_to_conds.end()) {
      std::vector<int> s;
      s.push_back(i);
      rel_to_conds.insert({rel_num, s});
    } else {
      rel_to_conds[rel_num].push_back(i);
    }
  }
  return Status::OK();
}

Status QL_Manager::GetAttrEntry(const RelAttr &attr, AttrEntry *&entry) {
  Status s;
  int slot;
  s = GetAttrEntryPos(attr, slot); if (!s.ok()) return s;
  entry = attr_entry_ + slot;
  return Status::OK();
}

Status QL_Manager::GetAttrEntryPos(const RelAttr &attr, int &slot) {
  std::string rel_name;
  if (attr.rel_name != nullptr) {
    std::string temp_string(attr.rel_name);
    rel_name = temp_string;
  } else {
    std::string attr_name(attr.attr_name);
    rel_name = attr_to_rel_[attr_name][0];
  }
  int rel_num = rel_to_int_[rel_name];
  int num_attrs = rel_entry_[rel_num].attrCount;
  int start = rel_to_first_attr_[rel_name];
  for (int i = 0; i < num_attrs; i++) {
    if (strcmp(attr.attr_name, attr_entry_[i + start].attrName) == 0) {
      slot = i + start;
      return Status::OK();
    }
  }
  return Status(ErrorCode::kQL, "attribute not found");
}

Status QL_Manager::SetUpNodes(QL_Node *&top_node, int num_sel_attrs,
                              const RelAttr sel_attrs[]) {
  Status s;
  s = SetUpFirstNode(top_node); if (!s.ok()) return s;

  for (int i = 1; i < num_rels_; i++) {
    s =  JoinRelation(top_node, i);
  }

  if (num_sel_attrs == 1
      && strncmp(sel_attrs[0].attr_name, "*", strlen(sel_attrs[0].attr_name))
          == 0) {
    return Status::OK();
  }

  QL_NodeProj *proj_node = new QL_NodeProj(*this, *top_node);
  int *attr_keep_list = new int[num_sel_attrs];
  for (int i = 0; i < num_sel_attrs; i++) {
    int slot;
    s = GetAttrEntryPos(sel_attrs[i], slot);
    attr_keep_list[i] = slot;
  }
  proj_node->SetUpNode(num_sel_attrs, attr_keep_list);
  delete[] attr_keep_list;
  top_node = proj_node;
  return Status::OK();
}



Status QL_Manager::SetUpFirstNode(QL_Node *&top_node) {
  Status s;
  QL_NodeRel *rel_node = new QL_NodeRel(*this, rel_entry_);
  top_node = rel_node;
  int rel_index = 0;
  int num_attrs = rel_entry_[rel_index].attrCount;
  int *attr_list = new int[num_attrs];
  std::string rel_name(rel_entry_->relName);
  int start = rel_to_first_attr_[rel_name];
  for (int i = 0; i < num_attrs; i++) {
    attr_list[i] = start + i;
  }
  rel_node->SetUpAttrList(attr_list, num_attrs);
  delete[] attr_list;
  std::vector <int> conds = rel_to_conds[0];
  bool use_index = false;
  bool use_sel = false;
  for (unsigned int i = 0; i < conds.size(); i++) {
    int n = conds[i];
    bool added = false;
    if (conditions_[n].op == EQ_OP && !use_index) {
      int slot;
      s = GetAttrEntryPos(conditions_[n].l_attr, slot); if (!s.ok()) return s;
      if (attr_entry_[slot].indexNo != -1) {
        rel_node->UseIndex(slot, attr_entry_[slot].indexNo, conditions_[n].r_value.data);
        use_index = true;
        added = true;
      }
    }
    if (!use_sel && !added) {
      QL_NodeSel *sel_node = new QL_NodeSel(*this, *rel_node);
      sel_node->SetUpNode(conds.size());
      top_node = sel_node;
      use_sel = true;
    }
    if (!added) {
      s = top_node->AddConditions(conditions_[n]); if (!s.ok()) return s;
    }
  }
  return Status::OK();
}

Status QL_Manager::JoinRelation(QL_Node *&top_node, int rel_index) {
  Status s;
  RelEntry *rel_entry = rel_entry_ + rel_index;
  QL_NodeRel *rel_node = new QL_NodeRel(*this, rel_entry);

  int *attr_list;
  attr_list = new int[rel_entry_->attrCount];
  int start = rel_to_first_attr_[rel_entry->relName];
  for (int i = 0; i < rel_entry->attrCount; i++) {
    attr_list[i] = start + i;
  }
  rel_node->SetUpAttrList(attr_list, rel_entry->attrCount);
  delete[] attr_list;

  int num_conds = rel_to_conds[rel_index].size();
  QL_NodeJoin *join_node = new QL_NodeJoin(*this, *top_node, *rel_node);
  join_node->SetUpNode(num_conds);
  top_node = join_node;

  bool use_index = false;
  for (unsigned int i = 0; i < rel_to_conds[rel_index].size(); i++) {
    int cond_num = rel_to_conds[rel_index][i];
    if (!use_index) {
      if (conditions_[cond_num].op == EQ_OP && !conditions_[cond_num].r_is_attr) {
        int slot;
        GetAttrEntryPos(conditions_[cond_num].l_attr, slot);
        if (attr_entry_[slot].indexNo != -1) {
          rel_node->UseIndex(slot, attr_entry_[slot].indexNo,
                             conditions_->r_value.data);
        }
        use_index = true;
        continue;
      } else if (conditions_[cond_num].op == EQ_OP && conditions_[cond_num].r_is_attr){
        int slot1, slot2;
        GetAttrEntryPos(conditions_[cond_num].l_attr, slot1);
        GetAttrEntryPos(conditions_[cond_num].r_attr, slot2);
        if (strcmp(attr_entry_[slot1].relName,rel_entry_[rel_index].relName) != 0) {
          int temp = slot1;
          slot1 = slot2;
          slot2 = temp;
        }
        if (attr_entry_[slot2].indexNo != -1) {
          s = join_node->UseIndexJoin(slot1, slot2, attr_entry_[slot2].indexNo);
          if (!s.ok()) return s;
          use_index = true;
          continue;
        }
      }
    }
    s = top_node->AddConditions(conditions_[cond_num]); if (!s.ok()) return s;
  }
  return Status::OK();
}

}  // namespace toydb
