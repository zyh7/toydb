/*
 * pf_filehandle.cc
 *
 *  Created on: Apr 7, 2018
 *      Author: zyh
 */

#include "pf.h"
#include "status.h"
#include "pf_buffermanager.h"
#include "pf_internal.h"
#include <string.h>
#include <sys/types.h>
#include <unistd.h>


namespace toydb {
FileHandle::FileHandle() {
  file_open_ = false;
  buffer_manager_ = nullptr;
}

FileHandle::~FileHandle() {

}

FileHandle::FileHandle(const FileHandle &f) {
   fd_ = f.fd_;
   file_open_ = f.file_open_;
   buffer_manager_ = f.buffer_manager_;
   header_changed_ = f.header_changed_;
   header_ = f.header_;
}

int FileHandle::IsValidPageNum(PageNum num) const {
  return (num >= 0 && num <header_.num_pages);
}

FileHandle& FileHandle::operator=(const FileHandle &f) {
  if (this != &f) {
    fd_ = f.fd_;
    file_open_ = f.file_open_;
    buffer_manager_ = f.buffer_manager_;
    header_changed_ = f.header_changed_;
    header_ = f.header_;
  }
  return *this;
}

Status FileHandle::GetPage(PageNum num, Page &p) {
  Status s;
  char *page_buf;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not getpage");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  s = buffer_manager_->GetPage(fd_, num, &page_buf);
  if (!s.ok()) return s;
  if (((PageHdr*)page_buf)->nextFree == -2) {
     p.data = page_buf + sizeof(PageHdr);
     p.num = num;
     return Status::OK();
  }
  s = buffer_manager_->UnpinPage(fd_, num);
  if (!s.ok()) return s;
  return Status(ErrorCode::kPF, "can not getpage, it is a free page");
}

Status FileHandle::AllocatePage(Page &p) {
  Status s;
  char *buf;
  PageNum num;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not allocate page");
  }
  if (header_.first_free != -1) {
    num = header_.first_free;
    s = buffer_manager_->GetPage(fd_, num, &buf);
    if (!s.ok()) return s;
    header_.first_free = ((PageHdr*)buf)->nextFree;
  } else {
    num = header_.num_pages;
    s = buffer_manager_->AllocatePage(fd_, num, &buf);
    if (!s.ok()) return s;
    header_.num_pages++;
  }
  header_changed_ = true;
  memset(buf, 0, kPageSize);
  ((PageHdr*)buf)->nextFree = -2;
  s = buffer_manager_->MarkDirty(fd_, num);
  if (!s.ok()) return s;
  p.data = buf + sizeof(PageHdr);
  p.num = num;
  return Status::OK();
}

Status FileHandle::DisposePage(PageNum num) {
  Status s;
  char *buf;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not allocate page");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  s = buffer_manager_->GetPage(fd_, num, &buf);
  if (((PageHdr*)buf)->nextFree != -2) {
    s = buffer_manager_->UnpinPage(fd_, num);
    if (!s.ok()) return s;
    return Status(ErrorCode::kPF, "page is already free.can not dispose");
  }
  ((PageHdr*)buf)->nextFree = header_.first_free;
  header_.first_free = num;
  header_changed_ = true;
  s = buffer_manager_->MarkDirty(fd_, num);
  if (!s.ok()) return s;
  s = buffer_manager_->UnpinPage(fd_, num);
  if (!s.ok()) return s;
  return Status::OK();
}

Status FileHandle::MarkDirty(PageNum num) const {
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not markdirty");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  return buffer_manager_->MarkDirty(fd_, num);
}

Status FileHandle::UnpinPage(PageNum num) const {
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not unpinpage");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  return buffer_manager_->UnpinPage(fd_, num);
}

Status FileHandle::FlushPages() {
  if (file_open_ == false) {
     return Status(ErrorCode::kPF, "file is not open.can not flushpage");
  }
  if (header_changed_ == true) {
    if (lseek(fd_, 0, SEEK_SET) < 0) {
      return Status(ErrorCode::kPF, "lseek failed when writing header");
    }
    int num_bytes = write(fd_, (char *)&header_,sizeof(FileHdr));
    if (num_bytes != sizeof(FileHdr)) {
      return Status(ErrorCode::kPF, "write header failed");
    }
    header_changed_  = false;
  }
  return buffer_manager_->FlushPages(fd_);
}

Status FileHandle::ForcePages(PageNum num) {
  if (file_open_ == false) {
     return Status(ErrorCode::kPF, "file is not open.can not flushpage");
  }
  if (header_changed_ == true) {
    if (lseek(fd_, 0, SEEK_SET) < 0) {
      return Status(ErrorCode::kPF, "lseek failed when writing header");
    }
    int num_bytes = write(fd_, (char *)&header_,sizeof(FileHdr));
    if (num_bytes != sizeof(FileHdr)) {
      return Status(ErrorCode::kPF, "write header failed");
    }
    header_changed_  = false;
  }
  return buffer_manager_->ForcePage(fd_, num);
}



}  // namespace toydb
