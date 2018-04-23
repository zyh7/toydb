/*
 * pf_hashtable.h
 *
 *  Created on: Apr 7, 2018
 *      Author: zyh
 */

#ifndef TOYDB_SRC_PF_HASHTABLE_H_
#define TOYDB_SRC_PF_HASHTABLE_H_

#include <pf.h>
#include <status.h>

namespace toydb {

struct HashEntry {
  HashEntry *prev;
  HashEntry *next;
  int fd;
  PageNum page_num;
  int slot;
};

class HashTable {
 public:
  HashTable(int num_buckets);
  ~HashTable();

  Status Insert(int fd, PageNum num, int slot);
  Status Delete(int fd,PageNum num);
  void Find(int fd,PageNum num, int &slot);
 private:
  int num_buckets_;
  HashEntry **hash_entry_;
  int Hash(int fd, PageNum num) { return ((fd + num)%num_buckets_); }
};

}  // namespace toydb

#endif  // STOYDB_SRC_PF_HASHTABLE_H
