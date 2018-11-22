/*
 * pf_filehandle.cc
 *
 *  Created on: Apr 7, 2018
 *      Author: zyh
 */

#include "pf.h"
#include "status.h"
#include "pf_buffermanager.h"
#include "pf_buffermanagerwal.h"
#include "pf_internal.h"
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "../include/wal.h"


namespace toydb {
PF_FileHandle::PF_FileHandle() {
  file_open_ = false;
  buffer_manager_ = nullptr;
}

PF_FileHandle::~PF_FileHandle() {

}

PF_FileHandle::PF_FileHandle(const PF_FileHandle &f) {
   fd_ = f.fd_;
   file_open_ = f.file_open_;
   buffer_manager_ = f.buffer_manager_;
   buffer_manager_wal_ = f.buffer_manager_wal_;
   header_changed_ = f.header_changed_;
   header_ = f.header_;
}

int PF_FileHandle::IsValidPageNum(PageNum num) const {
  return (num == -1 || (num > -1 && num <header_->num_pages));
}

PF_FileHandle& PF_FileHandle::operator=(const PF_FileHandle &f) {
  if (this != &f) {
    fd_ = f.fd_;
    file_open_ = f.file_open_;
    buffer_manager_ = f.buffer_manager_;
    header_changed_ = f.header_changed_;
    header_ = f.header_;
  }
  return *this;
}

Status PF_FileHandle::GetPage(PageNum num, Page &p) const{
  Status s;
  char *page_buf;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not getpage");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  PageInfo page_info(rel_id_, type_, num);
  if (use_wal_) {
    s = buffer_manager_wal_->GetPage(fd_, page_info, &page_buf);
  } else {
    s = buffer_manager_->GetPage(fd_, num, &page_buf);
  }
  if (!s.ok()) return s;
  if (((PF_PageHeader*)page_buf)->nextFree == -2 || num == -1) {
     p.data = page_buf + sizeof(PF_PageHeader);
     p.num = num;
     return Status::OK();
  }
  if (use_wal_) {
    s = buffer_manager_wal_->UnpinPage(fd_, page_info);
  } else {
    s = buffer_manager_->UnpinPage(fd_, num);
  }
  if (!s.ok()) return s;
  return Status(ErrorCode::kPF, "can not getpage, it is a free page");
}

Status PF_FileHandle::GetNextPage(PageNum current, Page &p, bool &eof) const {
  Status s;
  char *page_buf;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not get next page");
  }
  if (current == header_->num_pages) {
    eof = true;
    return Status::OK();
  }
  if (!IsValidPageNum(current)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  for (PageNum i = current; i < header_->num_pages; i++) {
    PageInfo page_info(rel_id_, type_, i);
    if (use_wal_) {
      s = buffer_manager_wal_->GetPage(fd_, page_info, &page_buf);
    }else {
      s = buffer_manager_->GetPage(fd_, i, &page_buf);
    }
    if (!s.ok()) return s;
    if (((PF_PageHeader*) page_buf)->nextFree == -2) {
      p.data = page_buf + sizeof(PF_PageHeader);
      p.num = i;
      eof = false;
      return Status::OK();
    }
    if (use_wal_) {
      s = buffer_manager_wal_->UnpinPage(fd_, page_info); if (!s.ok()) return s;
    } else {
      s = buffer_manager_->UnpinPage(fd_, i);  if (!s.ok()) return s;
    }
  }
  eof = true;
  return Status::OK();
}

Status PF_FileHandle::AllocatePage(Page &p) {
  Status s;
  char *buf;
  PageNum num;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not allocate page");
  }
  if (header_->first_free != -1) {
    num = header_->first_free;

    if (use_wal_) {
      PageInfo page_info(rel_id_, type_, num);
      s = buffer_manager_wal_->GetPage(fd_, page_info, &buf);
    } else {
      s = buffer_manager_->GetPage(fd_, num, &buf);
    }


    if (!s.ok()) return s;
    header_->first_free = ((PF_PageHeader*)buf)->nextFree;
  } else {
    num = header_->num_pages;
    if (use_wal_) {
      PageInfo page_info(rel_id_, type_, num);
      s = buffer_manager_wal_->AllocatePage(fd_, page_info, &buf);
    } else {
      s = buffer_manager_->AllocatePage(fd_, num, &buf);
    }
    if (!s.ok()) return s;
    header_->num_pages++;
  }
  header_changed_ = true;
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, -1);
    buffer_manager_wal_->MarkDirty(fd_, page_info);
  } else {
    buffer_manager_->MarkDirty(fd_, -1);
  }
  memset(buf, 0, kPageSize);
  ((PF_PageHeader*)buf)->nextFree = -2;
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, num);
    s = buffer_manager_wal_->MarkDirty(fd_, page_info);
  } else {
    s = buffer_manager_->MarkDirty(fd_, num);
  }
  if (!s.ok()) return s;
  p.data = buf + sizeof(PF_PageHeader);
  p.num = num;
  return Status::OK();
}

Status PF_FileHandle::DisposePage(PageNum num) {
  Status s;
  char *buf;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not dispose page");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  PageInfo page_info(rel_id_, type_, num);
  if (use_wal_) {
    s = buffer_manager_wal_->GetPage(fd_, page_info, &buf);
  } else {
    s = buffer_manager_->GetPage(fd_, num, &buf);
  }
  if (((PF_PageHeader*)buf)->nextFree != -2) {
    if (use_wal_) {
      s = buffer_manager_wal_->UnpinPage(fd_, page_info);
    } else {
      s = buffer_manager_->UnpinPage(fd_, num);
    }
    if (!s.ok()) return s;
    return Status(ErrorCode::kPF, "page is already free.can not dispose");
  }
  ((PF_PageHeader*)buf)->nextFree = header_->first_free;
  header_->first_free = num;
  header_changed_ = true;
  if (use_wal_) {
    page_info.page_num = -1;
    s = buffer_manager_wal_->MarkDirty(fd_, page_info);  if (!s.ok()) return s;
    page_info.page_num = num;
    s = buffer_manager_wal_->MarkDirty(fd_, page_info);  if (!s.ok()) return s;
    s = buffer_manager_wal_->UnpinPage(fd_, page_info);  if (!s.ok()) return s;
  } else {
    s = buffer_manager_->MarkDirty(fd_, -1);  if (!s.ok()) return s;
    s = buffer_manager_->MarkDirty(fd_, num);  if (!s.ok()) return s;
    s = buffer_manager_->UnpinPage(fd_, num);  if (!s.ok()) return s;
  }
  return Status::OK();
}

Status PF_FileHandle::MarkDirty(PageNum num) const {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not markdirty");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, num);
    s = buffer_manager_wal_->MarkDirty(fd_,page_info);
  } else {
    s = buffer_manager_->MarkDirty(fd_, num);
  }
  return s;
}

Status PF_FileHandle::UnpinPage(PageNum num) const {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kPF, "file is not open.can not unpinpage");
  }
  if (!IsValidPageNum(num)) {
    return Status(ErrorCode::kPF, "page num is invalid");
  }
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, num);
    s = buffer_manager_wal_->UnpinPage(fd_, page_info);
  } else {
    s = buffer_manager_->UnpinPage(fd_, num);
  }
  return s;
}

Status PF_FileHandle::FlushPages() {
  Status s;
  if (file_open_ == false) {
     return Status(ErrorCode::kPF, "file is not open.can not flushpage");
  }
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, -1);
    s = buffer_manager_wal_->UnpinPage(fd_, page_info); if (!s.ok()) return s;
    s = buffer_manager_wal_->UnpinPage(fd_, page_info); if (!s.ok()) return s;
  } else {
    s = buffer_manager_->UnpinPage(fd_, -1); if (!s.ok()) return s;
    s = buffer_manager_->FlushPages(fd_); if (!s.ok()) return s;
  }
  return Status::OK();
}

Status PF_FileHandle::ForcePages(PageNum num) {
  Status s;
  if (file_open_ == false) {
     return Status(ErrorCode::kPF, "file is not open.can not flushpage");
  }
  if (use_wal_) {
    PageInfo page_info(rel_id_, type_, num);
    s = buffer_manager_wal_->ForcePage(fd_, page_info);
  } else {
    s = buffer_manager_->ForcePage(fd_, num);
  }
  return s;
}

}  // namespace toydb
