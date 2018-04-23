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

class FileHandle{
 public:
  FileHandle();
  ~FileHandle();

  FileHandle(const FileHandle &f);
  FileHandle& operator=(const FileHandle &f);

  Status GetPage(PageNum num, Page &p);
  Status AllocatePage(Page &p);
  Status DisposePage(PageNum num);
  Status MarkDirty(PageNum num) const;
  Status UnpinPage(PageNum num) const;
  Status FlushPages();
  Status ForcePages(PageNum num=-1);

 private:
  friend class PFManager;
  int fd_;
  int file_open_;
  FileHdr header_;
  int header_changed_;
  BufferManager *buffer_manager_;

  int IsValidPageNum(PageNum n) const;
};

class PFManager{
 public:
  PFManager();
  ~PFManager();

  Status CreateFile(const char *fname);
  Status DeleteFile(const char *fname);
  Status OpenFile(const char *fname, FileHandle &fh);
  Status CloseFile(FileHandle &fh);

  // no copying allowed
  PFManager(const PFManager &p);
  PFManager& operator=(const PFManager &p);

 private:
  BufferManager *buffer_manager_;
};

}  // namespace toydb

#endif  // TOYDB_PF_H_
