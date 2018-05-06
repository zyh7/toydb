/*
 * pf_pfmanager.h
 *
 *  Created on: Apr 1, 2018
 *      Author: zyh
 */

#include "pf.h"
#include "pf_buffermanager.h"
#include "pf_internal.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace toydb {

PF_Manager::PF_Manager(){
  buffer_manager_ = new BufferManager(kBufferSize);
}

PF_Manager::~PF_Manager(){
  delete buffer_manager_;
}

Status PF_Manager::CreateFile(const char *fname) {
  int fd = open(fname, O_WRONLY);
  if (fd >= 0) {
    return Status(ErrorCode::kPF,"Can not create file. File already exists.");
  } else {
    fd = open(fname, O_CREAT | O_RDWR, 00600);
    FileHdr header;
    header.first_free = -1;
    header.num_pages = 0;
    int num_bytes = write(fd, (char *)&header, sizeof(FileHdr));
    if (num_bytes != sizeof(FileHdr)) {
      close(fd);
      unlink(fname);
      return Status(ErrorCode::kPF, "can not write header when creating file");
    }
    if (close(fd) < 0) {
      return Status(ErrorCode::kPF, "can not close file");
    }
  }
  return Status::OK();
}

Status PF_Manager::DeleteFile(const char *fname) {
  if (unlink(fname) < 0) {
    return Status(ErrorCode::kPF,"can not delete file");
  }
  return Status::OK();
}

Status PF_Manager::OpenFile(const char *fname,PF_FileHandle &fh){
  if (fh.file_open_) return Status(ErrorCode::kPF, "file is already open");
  int fd = open(fname, O_RDWR);
  if (fd < 0) {
    return Status(ErrorCode::kPF,"can not open file.File does not exist.");
  }
  int num_bytes = read(fd, (char *)&fh.header_, sizeof(FileHdr));
  if (num_bytes != sizeof(FileHdr)) {
    close(fd);
    return  Status(ErrorCode::kPF, "can not read header when opening file");
  }
  fh.fd_ = fd;
  fh.file_open_ = 1;
  fh.buffer_manager_ = buffer_manager_;
  return Status::OK();
}

Status PF_Manager::CloseFile(PF_FileHandle &fh) {
  Status s;
  if (!fh.file_open_) {
    return Status(ErrorCode::kPF,"Can not close file.File is not open.");
  }
  s = fh.FlushPages(); if(!s.ok()) return s;
  if (close(fh.fd_) < 0) {
    return Status(ErrorCode::kPF, "can not close file");
  }
  fh.file_open_ = false;
  fh.buffer_manager_ = nullptr;
  return Status::OK();
}

}  // namespace toydb
