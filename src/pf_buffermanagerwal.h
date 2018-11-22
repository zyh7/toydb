#ifndef TOYDB_SRC_PF_BUFFERMANAGERWAL_H_
#define TOYDB_SRC_PF_BUFFERMANAGERWAL_H_

#include "../include/wal.h"
#include "pf_hashtablewal.h"
#include "toydb.h"

namespace toydb {

struct BufPageDescWal {
  char *data;
  int fd;
  PageInfo page_info;
  int prev;
  int next;
  int dirty;
  int pin_count;
};

class BufferManagerWal {
 public:
  BufferManagerWal(int num_pages);
  ~BufferManagerWal();

  Status GetPage(int fd, const PageInfo &page_info, char **ppbuffer, int bMultiplePins = true);
  Status AllocatePage(int fd, PageInfo num, char **ppBuffer);
  Status MarkDirty(int fd, PageInfo num);
  Status UnpinPage(int fd, PageInfo num);
  Status FlushPages(int fd);
  Status ForcePage(int fd, PageInfo num);

 private:
  HashTableWal hash_table_;
  BufPageDescWal *buftable_;
  WAL_FileHandle wh_;
  int num_pages_;
  int page_size_;
  int head_;
  int tail_;
  int free_;

  void InsertFree(int slot);
  void LinkHead(int slot);
  void Unlink(int slot);
  Status AllocSlot(int &slot);
  Status ReadPage(int fd, PageInfo num, char *dest);
  Status WritePage(int fd, PageInfo num, char *source);
  void InitPageDesc (int fd, PageInfo num, int slot);
};


}  // namespace toydb

#endif  // TOYDB_SRC_PF_BUFFERMANAGERWAL_H_
