#include "wal_lock.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <assert.h>
#include <sys/mman.h>
#include "wal.h"
#include "wal_internal.h"
#include "pf.h"
#include "pf_buffermanagerwal.h"

namespace toydb{

int WAL_Manager::CreateWALFile(const char *db_name){
  std::string name(db_name);
  name = name + ".wal";
  int fd = open(name.c_str(), O_WRONLY);
  if (fd > 0) {
    return -1;
  }
  fd = open(name.c_str(), O_CREAT | O_RDWR, 00600);
  assert(fd > 0);
  WALFileHeader header;
  header.tail_frame = header.tail_frame1 = 1;
  header.written_tail_frame = header.written_tail_frame1 = 1;
  int num_bytes = write(fd, (char *) &header, sizeof(WALFileHeader));
  assert(num_bytes == sizeof(WALFileHeader));
  int rt = close(fd);
  assert(rt == 0);
  return 0;
}

int WAL_Manager::CreateLockFile(const char *db_name){
  std::string name(db_name);
  name = name + ".lock";
  int fd = open(name.c_str(), O_WRONLY);
  if (fd > 0) {
    return -1;
  }
  fd = open(name.c_str(), O_CREAT | O_RDWR, 00600);
  assert(fd > 0);
  LockFile header;
  memset(&header, 0, sizeof(LockFile));
  header.tail_frame = 1;
  header.written_tail_frame = 1;
  int num_bytes = write(fd, (char *) &header, sizeof(LockFile));
  assert(num_bytes == sizeof(LockFile));
  int rt = close(fd);
  assert(rt == 0);
  return 0;
}

int WAL_Manager::CreateWALIndex(const char *db_name){
  std::string name(db_name);
  name = name + ".hash";
  int fd = open(name.c_str(), O_WRONLY);
  if (fd > 0) {
    return -1;
  }
  fd = open(name.c_str(), O_CREAT | O_RDWR, 00600);
  assert(fd > 0);
  int rt = close(fd);
  assert(rt == 0);
  return 0;
}

int WAL_Manager::OpenDB(const char *db_name, PF_Manager &pfm) {
  return OpenDB(db_name, pfm.buffer_manager_wal_->wh_);
}


int WAL_Manager::OpenDB(const char *db_name, WAL_FileHandle &wh) {
  std::string name(db_name);
  std::string wal_name = name + ".wal";
  std::string lock_name = name + ".lock";
  std::string hash_name = name + ".hash";

  // open wal
  int fd1 = open(wal_name.c_str(), O_RDWR, 00600);
  assert(fd1 > 0);
  wh.fd_wal_ = fd1;

  // open hash
  int fd2 = open(lock_name.c_str(), O_RDWR, 00600);
  assert(fd2 > 0);
  wh.fd_lock_ = fd2;
  LockFile *lock = (LockFile *) mmap(nullptr, sizeof(LockFile),
                              PROT_READ | PROT_WRITE,
                                     MAP_SHARED, fd2, 0);
  wh.lock_ = lock;

  int head = lock->written_tail_frame;
  int tail = lock->tail_frame;


  lock_door(fd2);

  //first reader check whether frame is consistent between wal and index
  // in case of power failure
  if (!is_read_locked(fd2)){
    WALFileHeader wal_header;
    int num_bytes = read(fd1, &wal_header, sizeof(WALFileHeader));
    assert(num_bytes == sizeof(WALFileHeader));
    if (ValidateWalHeader(wal_header)) {
      num_bytes = write(fd1, &wal_header, sizeof(WALFileHeader));
      assert(num_bytes == sizeof(WALFileHeader));
      std::cout << "wal header correct" << std::endl;
    }

    if (wal_header.tail_frame != tail
        || wal_header.written_tail_frame != head) {
      int rt = wh.WriteBack(wal_header.written_tail_frame, wal_header.tail_frame);
      assert(rt == 0);

    }
  }

  lock_read(fd2);

  // now wal and index are valid
  bool find = false;
  for (int i = 0; i < kMaxReaders; i++) {
    if (lock->reader_frame[i] == 0) {  // pick an empty slot
      find = true;
      lock->reader_frame[i] = head;
      wh.head_frame_ = head;
      wh.tail_frame_ = tail;
      wh.initial_tail_frame_ = tail;
      int num_frames = tail -head;
      wh.head_table_ = (head - 1) / kNumEntryPerTable;
      if (tail % kNumEntryPerTable == 1) {
        wh.tail_table_ = (tail - 1) / kNumEntryPerTable;
      } else {
        wh.tail_table_ = (tail - 1) / kNumEntryPerTable + 1;
      }
      int num_tables = wh.tail_table_ - wh.tail_table_;
      int fd3 = open(hash_name.c_str(), O_RDWR, 00600);
      assert(fd3 > 0);
      wh.fd_hash_ = fd3;
      wh.table_list_ = (FrameHashTable *) mmap(
          nullptr, sizeof(FrameHashTable) * num_tables,
          PROT_READ | PROT_EXEC,
          MAP_SHARED, fd3, sizeof(FrameHashTable) * wh.head_table_);

      break;
    }
  }

  if (!find) {
    unlock_read(fd2);
    unlock_door(fd2);
    return -1;
  }

  unlock_door(fd2);
  return 0;
}

int WAL_Manager::ValidateWalHeader(WALFileHeader &header) {
  if (header.tail_frame != header.tail_frame1
      && header.written_tail_frame != header.written_tail_frame1) {
    std::cout << "wal header corrupt" << std::endl;
    header.tail_frame = header.tail_frame1 = std::max(header.tail_frame,
                                                      header.tail_frame1);
    header.written_tail_frame = header.written_tail_frame1 = std::max(
        header.written_tail_frame, header.written_tail_frame1);
    return -1;
  }
  return 0;
}

}  // namespace toydb



