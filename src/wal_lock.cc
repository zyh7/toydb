#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "wal_lock.h"

namespace toydb {

int lock_door (int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 0;
  flock.l_len = 1;
  flock.l_type = F_WRLCK;
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLKW, &flock);  // blocking
}
int unlock_door(int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 0;
  flock.l_len = 1;
  flock.l_type = F_UNLCK;
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLKW, &flock);  // blocking
}

int lock_write(int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 1;
  flock.l_len = 1;
  flock.l_type = F_WRLCK;
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLK, &flock);  // non-blocking
}

int unlock_write(int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 1;
  flock.l_len = 1;
  flock.l_type = F_UNLCK;
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLK, &flock);  // non-blocking
}


int lock_read(int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 2;
  flock.l_len = 1;
  flock.l_type = F_RDLCK;             // shared lock
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLK, &flock);  // non-blocking
}

int unlock_read(int fd) {
  flock flock;
  flock.l_pid = getpid();
  flock.l_start = 2;
  flock.l_len = 1;
  flock.l_type = F_RDLCK;             // shared lock
  flock.l_whence = SEEK_SET;
  return fcntl(fd, F_SETLK, &flock);  // non-blocking
}

bool is_read_locked(int fd) {
  flock lock;
  lock.l_pid = getpid();
  lock.l_start = 2;
  lock.l_len = 1;
  lock.l_whence = SEEK_SET;
  fcntl(fd, F_GETLK, &lock);
  if (lock.l_type == F_UNLCK) return false;
  else return true;
}

//int lock_frame(int fd) {
//  flock flock;
//  flock.l_pid = getpid();
//  flock.l_start = 3;
//  flock.l_len = 1;
//  flock.l_type = F_WRLCK;
//  flock.l_whence = SEEK_SET;
//  return fcntl(fd, F_SETLKW, &flock);  // blocking
//}
//
//int unlock_frame(int fd) {
//  flock flock;
//  flock.l_pid = getpid();
//  flock.l_start = 3;
//  flock.l_len = 1;
//  flock.l_type = F_UNLCK;
//  flock.l_whence = SEEK_SET;
//  return fcntl(fd, F_SETLKW, &flock);  // blocking
//}



}  // namespace toydb
