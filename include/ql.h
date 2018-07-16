/*
 * ql.h
 *
 *  Created on: Jun 2, 2018
 *      Author: zyh
 */

#ifndef TOYDB_QL_H_
#define TOYDB_QL_H_

#include <iostream>
#include <map>
#include <string>
#include <set>
#include <vector>
#include "toydb.h"
#include "sm.h"
#include "rm.h"
#include "ix.h"

namespace toydb {

struct RelAttr {
  char *rel_name;     // relation name (may be NULL)
  char *attr_name;    // attribute name
};

struct Value {
  AttrType type;
  void     *data;
};

struct Condition {
  RelAttr l_attr;
  CompOp  op;
  int     r_is_attr;
  RelAttr r_attr;
  Value   r_value;
};

class QL_Node;

class QL_Manager {
  friend class QL_Node;
  friend class QL_NodeProj;
  friend class QL_NodeJoin;
  friend class QL_NodeRel;
  friend class QL_NodeSel;
 public:
  QL_Manager(SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm);
  ~QL_Manager();
  Status Select(int num_sel_attrs, const RelAttr sel_attrs[], int num_relations,
                const char * const relations[], int num_conditions,
                const Condition conditions[]);
  Status Insert(const char *rel_name, int num_values, const Value values[]);
  Status Delete(const char *rel_name, int n_conditions,
                const Condition conditions[]);
  Status Update(const char *rel_name, const RelAttr &upd_attr, const int is_value,
                const RelAttr &r_attr, const Value &r_value,
                int num_conditions, const Condition conditions[]);
 private:
  SM_Manager &smm_;
  IX_Manager &ixm_;
  RM_Manager &rmm_;

  int num_rels_;
  int num_attrs_;
  int num_conds_;

  RelEntry *rel_entry_;
  AttrEntry *attr_entry_;
  const Condition *conditions_;

  std::map <std::string, int> rel_to_int_;
  std::map <std::string, int> rel_to_first_attr_;
  std::map<std::string, std::vector <std::string>> attr_to_rel_;
  std::map<int, std::vector <int>> rel_to_conds;


  void Reset();
  Status SetUpNodes(QL_Node *&top_node, int num_sel_attrs, const RelAttr sel_attrs[]);
  Status SetUpFirstNode(QL_Node *&top_node);
  Status RunSelect(QL_Node *top_node);
  Status RunDelete(QL_Node *top_node);
  Status CleanUpNodes(QL_Node *top_node);
  Status GetAttrEntry(const RelAttr &attr, AttrEntry *&entry);
  Status GetAttrEntryPos(const RelAttr &attr, int &slot);
  Status JoinRelation(QL_Node *&top_node, int rel_index);
  void PrintBuffer(std::ostream &c, void *buffer, int *attr_list, int num_attrs);
  Status SetUpOneRelation(const char *rel_name);
  Status CheckSeletAttrs(int num_attrs, const RelAttr attrs[]);
  Status CheckConditions(int num_conds, const Condition conditions[]);
  Status InsertIntoRelation(const char *rel_name, int tuple_len, int num_values,
                            const Value values[]);
  Status RunUpdate(QL_Node *top_node, const RelAttr &upd_attr, const int is_value,
                   const RelAttr &r_attr, const Value &r_value);
  bool IsValidAttr(const RelAttr &attr);
};

}  // namespace toydb

#endif // TOYDB_QL_H_
