#ifndef TOYDB_SRC_PF_HASHTABLEWAL_H_
#define TOYDB_SRC_PF_HASHTABLEWAL_H_

#include <pf.h>
#include "wal.h"
#include "wal_internal.h"

namespace toydb {

struct HashEntryWal {
  HashEntryWal *prev;
  HashEntryWal *next;
  PageInfo page_info;
  int slot;
};

class HashTableWal {
 public:
  HashTableWal(int num_buckets);
  ~HashTableWal();

  int Insert(const PageInfo &page_info, int slot);
  int Delete(const PageInfo &page_info);
  int Find(const PageInfo &page_info);

 private:
  int num_buckets_;
  HashEntryWal **hash_entry_;
  int Hash(const PageInfo &page_info);
};

}

#endif // TOYDB_SRC_PF_HASHTABLEWAL_H_
