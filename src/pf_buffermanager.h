/*
 * pf_buffermanager.h
 *
 *  Created on: Apr 7, 2018
 *      Author: zyh
 */

#ifndef TOYDB_SRC_PF_BUFFERMANAGER_H_
#define TOYDB_SRC_PF_BUFFERMANAGER_H_

#include "status.h"
#include "toydb.h"
#include "pf_hashtable.h"

namespace toydb {
struct BufPageDesc {
  char *data;
  int fd;
  PageNum page_num;
  int prev;
  int next;
  int dirty;
  int pin_count;
};

class BufferManager {
 public:
  BufferManager(int num_pages);
  ~BufferManager();

  Status GetPage(int fd, PageNum num, char **ppbuffer, int bMultiplePins = true);
  Status AllocatePage(int fd, PageNum num, char **ppBuffer);
  Status MarkDirty(int fd, PageNum num);
  Status UnpinPage(int fd, PageNum num);
  Status FlushPages(int fd);
  Status ForcePage(int fd, PageNum num);
//  Status ClearBuf();
//  Status PrintBuffer();
//  Status ResizeBuffer(int NewSize);
  void GetBlockSize(int &length) const;
//  Status AllocateBlock(char *&buffer);
//  Status DisposeBlock(char *buffer);

 private:
  HashTable hash_table_;
  BufPageDesc *buftable_;
  int num_pages_;
  int page_size_;
  int head_;
  int tail_;
  int free_;

  void InsertFree(int slot);
  void LinkHead(int slot);
  void Unlink(int slot);
  Status AllocSlot(int &slot);
  Status ReadPage(int fd, PageNum num, char *dest);
  Status WritePage(int fd, PageNum num, char *source);
  void InitPageDesc (int fd, PageNum num, int slot);

};

}

#endif  // TOYDB_SRC_PF_BUFFERMANAGER_H_
