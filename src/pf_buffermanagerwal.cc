#include "pf_buffermanagerwal.h"
#include "pf_internal.h"
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

namespace toydb {

BufferManagerWal::BufferManagerWal(int num_pages)
    : hash_table_(kNumBucketsWal) {
  num_pages_ = num_pages;
  page_size_ = kPageSize;
  buftable_ = new BufPageDescWal[num_pages];
  for (int i = 0; i < num_pages_; i++) {
    buftable_[i].data = new char[page_size_];
    memset(buftable_[i].data, 0, page_size_);
    buftable_[i].prev = i - 1;
    buftable_[i].next = i + 1;
  }
  buftable_[0].prev = buftable_[num_pages - 1].next = -1;
  free_ = 0;
  head_ = tail_ = -1;
}

BufferManagerWal::~BufferManagerWal() {
  for (int i = 0; i < num_pages_; i++) {
    delete[] buftable_[i].data;
  }
  delete[] buftable_;
}

void BufferManagerWal::InsertFree(int slot) {
  buftable_[slot].next = free_;
  free_ = slot;
}

void BufferManagerWal::LinkHead(int slot) {
  if (head_ != -1)
    buftable_[head_].prev = slot;
  buftable_[slot].prev = -1;
  buftable_[slot].next = head_;
  head_ = slot;
  if (tail_ == -1)
    tail_ = slot;
}

void BufferManagerWal::Unlink(int slot) {
  int prev = buftable_[slot].prev;
  int next = buftable_[slot].next;
  if (prev != -1) {
    buftable_[prev].next = next;
  } else {
    head_ = next;
  }
  if (next != -1) {
    buftable_[next].prev = prev;
  } else {
    tail_ = prev;
  }
}

Status BufferManagerWal::AllocSlot(int &slot) {
  if (free_ != -1) {
    slot = free_;
    free_ = buftable_[slot].next;
  } else {
    for (slot = tail_; slot != -1; slot = buftable_[slot].prev) {
      if (buftable_[slot].pin_count == 0)
        break;
    }
    if (slot == -1) {
      return Status(ErrorCode::kPF, "no slot is unpined");
    }
    if (buftable_[slot].dirty) {
      Status s = WritePage(buftable_[slot].fd, buftable_[slot].page_info,
                           buftable_[slot].data);
      if (!s.ok()) return s;
    }
    hash_table_.Delete(buftable_[slot].page_info);
    Unlink(slot);
  }
  LinkHead(slot);
  return Status::OK();
}

Status BufferManagerWal::ReadPage(int fd, PageInfo page_info, char *dest) {
  int rt1 = wh_.LoadPage(page_info.rel_id, page_info.type,
                            page_info.page_num, dest);
  if (rt1) {
    long offset = kFileHeaderSize + page_info.page_num * kPageSize;
    int rt = pread(fd, dest, kPageSize, offset);
    assert(rt > 0);
  }
//  if (lseek(fd, offset, SEEK_SET) < 0) {
//    return Status(ErrorCode::kPF, "lseek failed during readpage");
//  }
//  int num_bytes = read(fd, dest, kPageSize);
//  if (num_bytes != kPageSize) {
//    return Status(ErrorCode::kPF, "read failed during readpage");
//  }
  return Status::OK();
}

Status BufferManagerWal::WritePage(int fd, PageInfo page_info, char *source) {
  int rt = wh_.WritePage(page_info.rel_id, page_info.type, page_info.page_num,
                         source);
  assert(rt == 0);
  return Status::OK();
}

void BufferManagerWal::InitPageDesc(int fd, PageInfo page_info, int slot) {
  buftable_[slot].fd = fd;
  buftable_[slot].page_info = page_info;
  buftable_[slot].pin_count = 1;
  buftable_[slot].dirty = 0;
}

Status BufferManagerWal::GetPage(int fd, const PageInfo &page_info, char **ppbuffer,
                              int bMultiplePins) {
  Status s;
  int slot = hash_table_.Find(page_info);
  //if page is in buffer
  if (slot >= 0) {
    if (!bMultiplePins && buftable_[slot].pin_count > 0) {
      return Status(ErrorCode::kPF, "page is already pinned");
    }
    Unlink(slot);
    LinkHead(slot);
    buftable_[slot].pin_count++;
  } else {
    //if page is not in buffer
    s = AllocSlot(slot);
    if (!s.ok()) return s;
    hash_table_.Insert(page_info, slot);
    s = ReadPage(fd, page_info, buftable_[slot].data);
    if (!s.ok()) return s;
    InitPageDesc(fd, page_info, slot);
  }
  *ppbuffer = buftable_[slot].data;
  return Status::OK();
}

Status BufferManagerWal::AllocatePage(int fd, PageInfo page_info, char **ppbuffer) {

  Status s;
  int slot = hash_table_.Find(page_info);
  if (slot > 0) return Status(ErrorCode::kPF, "page is already in buffer");
  s = AllocSlot(slot);
  if (!s.ok()) return s;
  hash_table_.Insert(page_info, slot);
  if (!s.ok()) return s;
  InitPageDesc(fd, page_info, slot);
  *ppbuffer = buftable_[slot].data;
  return Status::OK();
}

Status BufferManagerWal::MarkDirty(int fd, PageInfo page_info) {
  int slot = hash_table_.Find(page_info);
  if (slot < 0) return Status(ErrorCode::kPF, "page not in hashtable");
  if (buftable_[slot].pin_count == 0) {
    return Status(ErrorCode::kPF, "page not pinned");
  }
  buftable_[slot].dirty = true;
  return Status::OK();
}

Status BufferManagerWal::UnpinPage(int fd, PageInfo page_info) {
  int slot = hash_table_.Find(page_info);
  if (slot < 0) return Status(ErrorCode::kPF, "page not in buffer");
  if (buftable_[slot].pin_count == 0) {
    return Status(ErrorCode::kPF, "page is already unpinned");
  }
  buftable_[slot].pin_count--;
  return Status::OK();
}

Status BufferManagerWal::FlushPages(int fd) {
  Status s;
  int slot = head_;
  while (slot != -1) {
    int next = buftable_[slot].next;
    if (buftable_[slot].fd == fd) {
      if (buftable_[slot].pin_count) {
        return Status(ErrorCode::kPF, "page is pinned. can not flushpage");
      }
      if (buftable_[slot].dirty) {
        s = WritePage(fd, buftable_[slot].page_info, buftable_[slot].data);
        if (!s.ok())
          return s;
      }
      hash_table_.Delete(buftable_[slot].page_info);
      if (!s.ok())
        return s;
      Unlink(slot);
      InsertFree(slot);
    }
    slot = next;
  }
  return Status::OK();
}

Status BufferManagerWal::ForcePage(int fd, PageInfo page_info) {
  Status s;
  int slot;
  if (page_info.page_num == -1) {
    slot = head_;
    while (slot != -1) {
      int next = buftable_[slot].next;
      if (buftable_[slot].fd == fd) {
        if (buftable_[slot].dirty) {
          s = WritePage(fd, buftable_[slot].page_info, buftable_[slot].data);
          if (!s.ok())
            return s;
          buftable_[slot].dirty = false;
        }
      }
      slot = next;
    }
  } else {
    slot = hash_table_.Find(page_info);
    if (slot == -1) return Status(ErrorCode::kPF,
                                  "can not force page,page not in buffer");
    if (buftable_[slot].dirty) {
      s = WritePage(fd, buftable_[slot].page_info, buftable_[slot].data);
      if (!s.ok())
        return s;
      buftable_[slot].dirty = false;
    }

  }
  return Status::OK();
}


}  // namespace toydb


