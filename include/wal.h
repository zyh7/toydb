#ifndef TOYDB_WAL_H_
#define TOYDB_WAL_H_

#include "toydb.h"
#include "status.h"

namespace toydb{

class LockFile;

static const int kNumEntryPerTable = 512;
static const int kNumSlotPerTable = 2 * kNumEntryPerTable;

struct WALFileHeader {   //头部只有写进程会修改，而写锁限制了同一时刻只有一个写进程。不存在竞争。
  int written_tail_frame;  // 写回到的frame
  int tail_frame;  //最新的已经提交了的事务的最后一页（-1表示日志为空)
  // check if crash happen during writing header
  // so as to restore or rewrite
  int written_tail_frame1;
  int tail_frame1;
};

struct FrameEntry{
  int frame_num;  // 0 if entry is empty for frame_num starts from 1
  int rel_id;  // 0,1, 2,3 ...
  int type; // -1 for record file  0,1,2,3...  for index file
  PageNum page_num;
};

struct PageInfo{
  int rel_id;
  int type;
  PageNum page_num;

  PageInfo() {}

  ~PageInfo() {}

  PageInfo(int rel_id, int type, PageNum page_num)
      : rel_id(rel_id),
        type(type),
        page_num(page_num) {
  }

  bool operator==(const PageInfo &page_info) {
    return (rel_id = page_info.rel_id && type == page_info.type
        && page_num == page_info.page_num);
  }

  PageInfo& operator=(const PageInfo &page_info) {
    rel_id = page_info.rel_id;
    type = page_info.type;
    page_num = page_info.page_num;
    return *this;
  }
};

struct FileEntry {
  int rel_id;
  int type;
};

struct Frame {
  FrameEntry entry;
  char page[4096];
};

struct FrameHashTable{
  FrameEntry entrys[kNumSlotPerTable];  // at least half is empty
};

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
  int Hash(int rel_id, PageNum page_num, int type);
  int Find(FrameHashTable *table, int rel_id, PageNum page_num, int type);  // return frame number
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
  int OpenDB(const char *db_name, WAL_FileHandle &wh);
  int CloseDB(WAL_FileHandle &wh);
private:
  int ValidateWalHeader(WALFileHeader &header);
  // no copying allowed
  WAL_Manager(const WAL_Manager &w);
  WAL_Manager& operator=(const WAL_Manager &w);
};


}

#endif /* TOYDB_WAL_H_ */
