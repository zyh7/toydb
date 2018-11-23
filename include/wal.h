#ifndef TOYDB_WAL_H_
#define TOYDB_WAL_H_

#include "toydb.h"
#include "status.h"

namespace toydb{

class LockFile;
class WALFileHeader;
class FrameHashTable;
class PF_Manager;

class WAL_FileHandle{
 public:
  WAL_FileHandle();
  ~WAL_FileHandle();
  int LoadPage(int rel_id, int type, PageNum page_num, char *dest);
  int WritePage(int rel_id, int type, PageNum page_num, char *source);
  int GetWriteLock();
  int Commit();
  int Rollback();
 private:
  friend class WAL_Manager;
  int Hash(int rel_id, int type, PageNum page_num);
  int Find(FrameHashTable *table, int rel_id, int type, PageNum page_num);  // return frame number
  int WriteBack(int head_frame, int tail_frame);
  int NumFrames();
  int NumTables();

  int fd_wal_;
  int fd_lock_;
  int fd_hash_;
  int head_frame_;
  int tail_frame_;
  int initial_tail_frame_;
  int head_table_;
  int tail_table_;

  LockFile *lock_;
  FrameHashTable *table_list_;
};

class WAL_Manager{
 public:
  WAL_Manager() {};
  ~WAL_Manager() {};

  int CreateWALFile(const char *db_name);
  int CreateLockFile(const char *db_name);
  int CreateWALIndex(const char *db_name);
  int DeleteFile(const char *fname);
  int OpenDB(const char *db_name, PF_Manager &pfm);
  int CloseDB(WAL_FileHandle &wh);
private:
  int OpenDB(const char *db_name, WAL_FileHandle &wh);
  int ValidateWalHeader(WALFileHeader &header);
  // no copying allowed
  WAL_Manager(const WAL_Manager &w);
  WAL_Manager& operator=(const WAL_Manager &w);
};


}

#endif /* TOYDB_WAL_H_ */
