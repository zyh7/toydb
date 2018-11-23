#ifndef TOYDB_SRC_WAL_INTERNAL_H_
#define TOYDB_SRC_WAL_INTERNAL_H_

#include "toydb.h"
#include "pf_internal.h"

namespace toydb {

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
    return (rel_id == page_info.rel_id && type == page_info.type
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
  PageInfo entry;
  char page[kPageSize];
};

struct FrameHashTable{
  FrameEntry entrys[kNumSlotPerTable];  // at least half is empty
};

}  // namespace toydb

#endif // TOYDB_SRC_WAL_INTERNAL_H_
