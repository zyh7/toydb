/*
 * ql_node.h
 *
 *  Created on: Jun 4, 2018
 *      Author: zyh
 */

#ifndef TOYDB_SRC_QL_NODE_H_
#define TOYDB_SRC_QL_NODE_H_

#include "rm.h"
#include "ix.h"
#include "ql.h"

namespace toydb {

class QL_Manager;
struct Condition;


struct Cond {
  AttrType attr_type;
  int length;
  int offset1;
  bool is_attr;
  void *value;
  int offset2;
  bool (* comparator_) (void *, void *, AttrType, int);
};

class QL_Node {
 public:
  QL_Node(QL_Manager &qlm);
  virtual ~QL_Node();
  virtual Status OpenIt() = 0;
  virtual Status OpenIt(void *value) = 0;
  virtual Status CloseIt() = 0;
//  virtual Status CloseIt() = 0;
  virtual Status DeleteIt() = 0;
  virtual Status GetNext(void *data, bool &eof) = 0;
  virtual Status GetNextRec(Record &rec, bool &eof) = 0;
//  virtual Status PrintBuffer() = 0;
  void GetTupleLength(int &length);
  virtual void GetAttrList(int *&attr_list, int &num_attrs);
  Status AddConditions(const Condition &cond);
  void CheckConditions(void *buffer,bool &is_ok);
  Status AttrSlotToOffset(int slot, int &offset);
  virtual void UseIndex(int attrNum, int indexNumber, void *data) = 0;

 protected:
  QL_Manager &qlm_;
  int tuple_len_;
  int *attr_list_;
  int num_attrs_;
  Cond *cond_list_;
  int cond_num_;
  bool node_set_;
};

class QL_NodeProj : public QL_Node {
 public:
  QL_NodeProj(QL_Manager &qlm, QL_Node &prev_node);
  ~QL_NodeProj();
  void SetUpNode(int num_attrs_keep, int *attr_keep_list);
  Status OpenIt();
  Status OpenIt(void *value);
  Status GetNextRec(Record &rec, bool &eof);
  Status CloseIt();
  Status DeleteIt();
  Status GetNext(void *data, bool &eof);
  void GetAttrList(int *&attr_list, int &num_attrs);  // override
  void UseIndex(int attrNum, int indexNumber, void *data);

 private:
  int num_attrs_keep_;
  int *attr_keep_list_;
  int *attr_keep_offset_;
  int *attr_len_;

  QL_Node &prev_node_;
  void *buffer_;
};

class QL_NodeJoin : public QL_Node {
 public:
  QL_NodeJoin(QL_Manager &qlm, QL_Node &node1, QL_Node &node2);
  ~QL_NodeJoin();

  Status OpenIt();
  Status CloseIt();
  Status DeleteIt();
  Status OpenIt(void *value);
  Status CheckConditon(bool &is_ok);
  Status GetNext(void *data, bool &eof);
  Status GetNextRec(Record &rec, bool &eof);
  void UseIndex(int attrNum, int indexNumber, void *data);
  void SetUpNode(int num_conds);
  Status UseIndexJoin(int left_attr, int right_attr, int index_num);
 private:
  int num_conds_;
  QL_Node &node1_;
  QL_Node &node2_;
  void *buffer_;
  int first_tuple_len_;
  bool got_first_tuple_;

  bool use_index_join_;
  int index_attr_;
};

class QL_NodeSel : public QL_Node {
 public:
  QL_NodeSel(QL_Manager &qlm, QL_Node &prev_node);
  ~QL_NodeSel();

  void SetUpNode(int num_conds);
  Status OpenIt();
  Status OpenIt(void *value);
  Status CloseIt();
  Status DeleteIt();
  Status GetNext(void *data, bool &eof);
  Status GetNextRec(Record &rec, bool &eof);
  void UseIndex(int attrNum, int indexNumber, void *data);


 private:
  QL_Node &prev_node_;
  void *buffer_;

};

class QL_NodeRel : public QL_Node {
 public:
  QL_NodeRel(QL_Manager &qlm, RelEntry *r_entry);
  ~QL_NodeRel();

  Status OpenIt();
  Status OpenIt(void *value);
  Status CloseIt();
  Status DeleteIt();
  Status GetNext(void *data, bool &eof);
  Status GetNextRec(Record &rec, bool &eof);
  void SetUpAttrList(int *attr_list, int num_attrs);
  void UseIndex(int index_attr, int index_no, void *data);

 private:
  bool use_index_;
  int index_attr_;
  int index_no_;
  void *value_;

  RelEntry *r_entry_;
  RM_FileHandle fh_;
  RM_FileScan fs_;
  IX_IndexHandle ih_;
  IX_IndexScan is_;

};

}  // namespace toydb

#endif  //TOYDB_SRC_QL_NODE_H_
