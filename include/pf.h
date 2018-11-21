/*
 * pf.h
 *
 *  Created on: Mar 28, 2018
 *      Author: zyh
 */

#ifndef TOYDB_PF_H_
#define TOYDB_PF_H_

#include "toydb.h"
#include "status.h"

namespace toydb{

class WAL_FileHandle;

class BufferManager;

struct Page {
  char *data;
  PageNum num;
  Page() : data(nullptr), num(-1) { }
};

struct FileHdr {
   int first_free;
   int num_pages;
};

class PF_FileHandle{
 public:
  PF_FileHandle();
  ~PF_FileHandle();

  PF_FileHandle(const PF_FileHandle &f);
  PF_FileHandle& operator=(const PF_FileHandle &f);

  Status GetPage(PageNum num, Page &p) const;
  // get the used page at or after page num current
  Status GetNextPage(PageNum current, Page &p, bool &eof) const;
  Status AllocatePage(Page &p);
  Status DisposePage(PageNum num);
  Status MarkDirty(PageNum num) const;
  Status UnpinPage(PageNum num) const;
  Status FlushPages();
  Status ForcePages(PageNum num=-1);

  Status SetWalMode(const WAL_FileHandle &WAL_FileHandle);

 private:
  friend class PF_Manager;
  int fd_;
  int file_open_;
  FileHdr *header_;
  int header_changed_;
  BufferManager *buffer_manager_;
  WAL_FileHandle *wal_handle;

  int IsValidPageNum(PageNum n) const;
};

class PF_Manager{
 public:
  PF_Manager();
  ~PF_Manager();

  Status CreateFile(const char *fname);
  Status DeleteFile(const char *fname);
  Status OpenFile(const char *fname, PF_FileHandle &fh);
  Status CloseFile(PF_FileHandle &fh);

  // no copying allowed
  PF_Manager(const PF_Manager &p);
  PF_Manager& operator=(const PF_Manager &p);

 private:
  BufferManager *buffer_manager_;
};

}  // namespace toydb

#endif  // TOYDB_PF_H_
