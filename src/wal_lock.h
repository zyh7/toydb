#ifndef TOYDB_SRC_WAL_H_
#define TOYDB_SRC_WAL_H_

#include "wal.h"

namespace toydb {

static const int kMaxReaders = 20;

struct LockFile {
  unsigned char door_lock;
  unsigned char write_lock;  // 写锁
  unsigned char read_lock_shared;  // to check if reader really exists
  // no need to make sure head and tail frame update at the same time
//  unsigned char frame_num_lock;
  int tail_frame;
  int written_tail_frame;
  // reader_frame is invalid after crash so read_lock_shared is needed to check if
  // reader exists.
  int reader_frame[kMaxReaders];  // 0 if not occupied because frame starts from 1
};

int lock_door (int fd);
int unlock_door(int fd);
int lock_write(int fd);
int unlock_write(int fd);
int lock_read(int fd);
int unlock_read(int fd);
bool is_read_locked(int fd);
//int lock_frame(int fd);
//int unlock_frame(int fd);

}  // namespace toydb
#endif /* TOYDB_SRC_WAL_H_ */
