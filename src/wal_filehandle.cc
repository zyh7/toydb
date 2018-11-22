#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <map>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <algorithm>
#include <assert.h>
#include "wal.h"
#include "wal_lock.h"
#include "wal_internal.h"
#include "pf_internal.h"

namespace toydb {

bool operator<(FileEntry const& left, FileEntry const& right) {
  if (left.rel_id < right.rel_id) return true;
  if (left.rel_id > right.rel_id) return false;
  return left.type < right.type;
}

WAL_FileHandle::WAL_FileHandle() {}

WAL_FileHandle::~WAL_FileHandle() {}

int WAL_FileHandle::Hash(int rel_id, PageNum page_num, int type) {
  return (rel_id * 92083 + page_num *69061 + type *29123) % kNumSlotPerTable;
}

int WAL_FileHandle::LoadPage(int rel_num,  int type, PageNum page_num, char *dest) {
  int frame = 0;
  for (int i = NumTables() - 1; i >=0; i--) {
    FrameHashTable *table = table_list_ + i;
    int frame = Find(table,page_num, type, rel_num);
    if (frame != 0) break;
  }
  if (frame != 0) {
    pread(fd_wal_, dest, kPageSize, sizeof(WALFileHeader) + sizeof(Frame) * (frame - 1) + sizeof(FrameEntry));
    return 0;
  } else {
    return -1;
  }
}



int WAL_FileHandle::GetWriteLock() {
   return lock_write(fd_lock_);
}

int WAL_FileHandle::WritePage(int rel_id,  int type, PageNum page_num,char *source) {
  FrameEntry entry;
  entry.rel_id = rel_id;
  entry.type = type;
  entry.page_num = page_num;
  int offset  = sizeof(WALFileHeader) + sizeof(Frame) * (tail_frame_ - 1);
  pwrite(fd_wal_, &entry, sizeof(FrameEntry), offset);
  pwrite(fd_wal_, source, kPageSize, offset + sizeof(FrameEntry));
  tail_frame_++;
  if ((tail_frame_ - 1) % kNumEntryPerTable == 0) {  // current table is full
    munmap((void *)table_list_, sizeof(FrameHashTable) * NumTables());
    FrameHashTable *empty_table = new FrameHashTable();
    memset(&empty_table, 0, sizeof(FrameHashTable));
    pwrite(fd_hash_, &empty_table, sizeof(FrameHashTable), sizeof(FrameHashTable) * NumTables());
    delete empty_table;
    tail_table_++;
    table_list_ = (FrameHashTable *) mmap(
        nullptr, sizeof(FrameHashTable) * NumTables(), PROT_READ | PROT_EXEC,
        MAP_SHARED, fd_hash_, sizeof(FrameHashTable) * head_table_);
  }
  int slot = Hash(rel_id, page_num, type);
  while(1) {
    FrameEntry *entry = &((table_list_ + NumTables() - 1)->entrys[slot]);
    if (entry->rel_id == 0) {
      entry->rel_id = rel_id;
      entry->page_num = page_num;
      entry->type = type;
      entry->frame_num = tail_frame_;
      break;
    }
    slot = (slot + 1) % kNumSlotPerTable;
  }
  return 0;
}

int WAL_FileHandle::Find(FrameHashTable *table, int rel_id, PageNum page_num, int type) {
  int slot = Hash(rel_id, page_num, type);
  int frame = 0;
  while(1) {
    FrameEntry *entry = &(table->entrys[slot]);
    if (entry->rel_id == rel_id && entry->type == type && entry->page_num == page_num) {
      frame = entry->frame_num;
    } else if (entry->frame_num > tail_frame_ || entry->rel_id == 0) {
    // new frame entry is seen as empty;
      break;
    }
    slot = (slot + 1) % kNumSlotPerTable;
  }
  return frame;
}

int WAL_FileHandle::NumFrames() {
  return tail_frame_ - head_frame_;
}

int WAL_FileHandle::NumTables() {
  return tail_table_ - head_table_;
}

int WAL_FileHandle::WriteBack(int head_frame, int tail_frame) {
  assert(head_frame < tail_frame);
  std::map<FileEntry, int> map_fd;
  Frame *frame = new Frame();
  for (int i = head_frame; i < tail_frame; i++) {
    pread(fd_wal_, (void *) frame, sizeof(Frame), sizeof(WALFileHeader) + sizeof(Frame) * (i - 1));
    FileEntry file_entry;
    file_entry.rel_id = frame->entry.rel_id;
    file_entry.type = frame->entry.type;
    auto it = map_fd.find(file_entry);
    int fd;
    if (it == map_fd.end()) {
      std::string str1 = std::to_string(file_entry.rel_id);
      std::string str2 = std::to_string(file_entry.type);
      std::string name = str1 + "." + str2;
      fd = open(name.c_str(), O_RDWR, 00600);
      map_fd[file_entry] = fd;
    } else {
      fd = it->second;
    }
    pwrite(fd, &(frame->page), kPageSize, kPageSize * frame->entry.page_num);
  }
  auto it = map_fd.begin();
  while(it != map_fd.end()) {
    int fd = it->second;
    fsync(fd);
    it++;
  }
  return 0;
}

int WAL_FileHandle::Commit() {
  lock_door(fd_lock_);
  bool erase = false;
  int min = INT_MAX;
  for (int i = 0; i < kMaxReaders; i++) {
    if(lock_->reader_frame[i] == head_frame_ && !erase) {  // erase its mark
      lock_->reader_frame[i] = 0;
      erase = true;
    } else if (lock_->reader_frame[i] != 0){
      min = std::min(min, lock_->reader_frame[i]);
    }
  }
  assert(erase);
  WALFileHeader header;
  if (min == INT_MAX) {  // no other reader, write back all
    WriteBack(lock_->written_tail_frame, lock_->tail_frame);
    // change header after writing wal file back to main db so that:
    // 1.crash during writing back to disk, header is valid
    // 2.even if crash during writing header, we can check
    header.tail_frame = header.tail_frame1 = 1;
    header.written_tail_frame = header.written_tail_frame1 = 1;
    pwrite(fd_wal_, &header, sizeof(WALFileHeader), 0);
    // sync to ensure commit
    fsync(fd_wal_);
    // sync header before change wal data in case data is sync but header is not
    // in that case wal file is broken.
    // After header is set to a new file header state,
    // data is ignored, no need to sync.
    ftruncate(fd_wal_, sizeof(WALFileHeader));
    lock_->tail_frame = 1;
    lock_->written_tail_frame = 1;
    ftruncate(fd_hash_, sizeof(FrameHashTable));
    FrameHashTable *empty = new FrameHashTable();
    memset(empty, 0, sizeof(FrameHashTable));
    pwrite(fd_hash_, (void *)empty, sizeof(FrameHashTable), 0);
    unlock_door(fd_lock_);
  } else {  // other reader exists, write back part
    unlock_door(fd_lock_);          // no need to lock door
    WriteBack(lock_->written_tail_frame, min);
    header.written_tail_frame = header.written_tail_frame1 = min;
    header.tail_frame = header.tail_frame1 = tail_frame_;
    pwrite(fd_wal_, &header, sizeof(WALFileHeader), 0);
    fsync(fd_wal_);  // ensure commit;
    lock_->tail_frame = tail_frame_;
    lock_->written_tail_frame = min;
  }
  unlock_write(fd_lock_);
  return 0;
}

int WAL_FileHandle::Rollback() {
  lock_door(fd_lock_);
  bool erase = false;
  for (int i = 0; i < kMaxReaders; i++) {
    if(lock_->reader_frame[i] == head_frame_) {
      lock_->reader_frame[i] = 0;
      erase = true;
    }
  }
  assert(erase);
  unlock_read(fd_lock_);
  unlock_door(fd_lock_);
  return 0;
}

}  // namespace toydb
