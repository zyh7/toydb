/*
 * ix.h
 *
 *  Created on: May 8, 2018
 *      Author: zyh
 */

#ifndef TOYDB_IX_H_
#define TOYDB_IX_H_

#include "toydb.h"
#include "status.h"
#include "rm.h"

namespace toydb {

struct IX_FileHeader {
  AttrType attr_type;
  int attr_length;

  // node
  int entry_offset_n;
  int key_offset_n;
  int max_keys_n;

  // bucket
  int entry_offset_b;
  int max_keys_b;

  PageNum root_page;
};

struct NodeHeader {
  bool is_leaf;
  int num_keys;
  int first_slot;
  int free_slot;
  PageNum invalid1;
  PageNum invlaid2;
};

struct NodeHeader_I {
  bool is_leaf;
  int num_keys;
  int first_slot;
  int free_slot;
  PageNum first_page;
  PageNum invalid;
};

struct NodeHeader_L {
  bool is_leaf;
  int num_keys;
  int first_slot;
  int free_slot;
  PageNum prev_page;
  PageNum next_page;
};

struct BucketHeader {
  int num_keys;
  int first_slot;
  int free_slot;
  PageNum next_bucket;
};

struct NodeEntry {
  bool is_duplicate;
  int next_slot;
  PageNum page;
  SlotNum slot;
};

struct BucketEntry {
  int next_slot;
  PageNum page;
  SlotNum slot;
};

class IX_IndexHandle {
 public:
  IX_IndexHandle();
  ~IX_IndexHandle();

  Status InsertEntry(void *value, const RID &rid);
  Status DeleteEntry(void *value, const RID &rid);
  Status ForcePages();

 private:
  friend class IX_Manager;
  friend class IX_IndexScan;

  bool index_open_;
  bool header_changed_;
  IX_FileHeader header_;
  PF_FileHandle pfh_;
  int (*comparator_) (void *, void *, int);

  // variables for locating current scan position
  bool scan_start_;
  bool first_entry_;  // whether it locates the first entry
  bool leaf_open_;  // whether leaf page is pinned
  PageNum curr_leaf_num_;  // entry
  NodeHeader_L *curr_leaf_header_;
  NodeEntry *leaf_entry_;
  char *key_;
  int leaf_slot_num_;
  bool bucket_open_;  // bucket
  PageNum curr_bucket_num_;
  int bucket_slot_num_;
  BucketHeader *curr_bucket_header_;
  BucketEntry *bucket_entry_;

  Status InsertIntoNonFullNode(NodeHeader *node_header, const RID &rid,
                               void *value);
  Status CreateNode(PageNum &page_num);
  Status SplitNode(NodeHeader *parent_header, NodeHeader *header, PageNum page,
                   int index, int &new_index, PageNum &new_page);
  Status CreateBucket(PageNum &page_num);
  Status InsertIntoBucket(PageNum page_num, const RID &rid);
  Status DeleteFromLeafNode(NodeHeader_L *header, void *value, const RID &rid,
                            bool &empty, bool &dirty);
  Status DeleteFromBucketList(PageNum first_bucket, const RID &rid, bool &is_last_rid,
                              RID &last_rid, PageNum &new_first_bucket, bool &new_bucket);
  Status DeleteFromInternalNode(NodeHeader_I *header, void *value, const RID &rid,
                                bool &empty, bool &dirty);
  void FindIndexInNode(NodeHeader *node_header, void *value, int &index,
                       int &prev_index, bool &is_dup);

  // for scanning
  Status GoToFirstEntry();
  Status GoToAppropriateEntry(bool equal,void *value);
  Status OpenFirstBucket();
  Status GetNextRIDAndValue(RID &rid, void *&value, bool &eof);
  Status CloseScan();
};

class IX_IndexScan {
 public:
  IX_IndexScan();
  ~IX_IndexScan();
  Status OpenScan(IX_IndexHandle &ixh, CompOp comp_op, void *value);
  Status BeginScan();
  Status GetNextEntry(RID &rid, bool &eof);
  Status CloseScan();
 private:
  bool scan_open_;
  bool scan_end_;
  IX_IndexHandle *ixh_;

  int attr_len_;
  AttrType attr_type_;
  bool (*comparator_) (void *, void *, AttrType, int);
  CompOp comp_op_;
  void *value_;

  bool equal_;
  bool use_first_leaf;
};

class IX_Manager {
 public:
  IX_Manager(PF_Manager &pfm);
  ~IX_Manager();
  Status CreateIndex(const char *rel_id, int index_num, AttrType attr_type,
                     int attr_length);
  Status DestroyIndex(const char *rel_id, int index_num);
  Status OpenIndex(const char *rel_id, int index_num, IX_IndexHandle &ixh);
  Status CloseIndex(IX_IndexHandle &ixh);

 private:
  PF_Manager &pfm_;

  bool IsValidIndex(AttrType attr_type, int attr_length);
  Status GetIndexFileName(const char *file_name, int index_num,
                          std::string &ix_name);
};

}

#endif  // TOYDB_IX_H_
