/*
 * rm.h
 *
 *  Created on: Apr 24, 2018
 *      Author: zyh
 */

#ifndef TOYDB_RM_H_
#define TOYDB_RM_H_

#include "toydb.h"
#include "status.h"
#include "pf.h"

namespace toydb{

struct RID {
  PageNum page_num;
  SlotNum slot_num;

  RID() : page_num(-1), slot_num(-1) { }
  RID(PageNum p_num, SlotNum s_num) : page_num(p_num), slot_num(s_num) { }

  RID& operator=(const RID &rid);
  bool operator==(const RID &rid) const;
};

class Record {
 public:
  RID rid_;
  char *data_;

  Record() : rid_(), data_(nullptr) { }
  ~Record();
  void SetRecord(const RID &rid, const char *data, int size);
};

struct RM_PageHeader {
  PageNum next_free;
  int num_records;
};

struct RM_FileHeader {
  PageNum next_free_page;
  int record_size;
  int num_record_per_page;
  int bitmap_offset;
  int num_pages;
  int bitmap_size;
};

class RM_FileHandle {
 public:
  RM_FileHandle();
  ~RM_FileHandle();
  Status GetRec(const RID &rid, Record &rec) const;
  Status InsertRec(const char *pData, RID &rid);
  Status DeleteRec(const RID &rid);
  Status UpdateRec(const Record &rec);
  Status ForcePages(PageNum page_num = -1);

 private:
  friend class RM_Manager;
  friend class RM_FileScan;

  RM_FileHeader header_;
  int file_open_;
  int header_changed_;
  PF_FileHandle pfh_;


  bool IsValidRID(const RID &rid) const;
  bool GetBit(const char *bitmap, int bit_num) const;
  bool GetNextOneBit(const char *bitmap, int size,int &bit_num) const;
  Status AllocateNewPage(Page &p);
  Status GetNextRec(RID &rid, Record &rec, bool &eof) const;
  Status GetPageHeaderAndBitmap(const Page &p, RM_PageHeader *&header, char *&bitmap) const;
  Status ResetBitmap(char *bitmap);
  void SetBit(char *bitmap, int bit_num);
  void ResetBit(char *bitmap, int bit_num);
  Status FindFirstZeroBit(const char *bitmap, int &slot_num);

};

class RM_FileScan {
 public:
  RM_FileScan();
  ~RM_FileScan();

  Status OpenScan(RM_FileHandle &fh, AttrType attr_type, int attr_length,
                  int attr_offset, CompOp comp_op, void *value);
  Status GetNextRec(Record &rec, bool &eof);
  Status CloseScan();
 private:
  int open_scan_;
  int scan_end_;
  AttrType attr_type_;
  int attr_length_;
  int attr_offset_;
  CompOp comp_op_;
  void *value_;
  RM_FileHandle *fh_;

  bool (*comparator_) (void *, void*, int, AttrType);

  RID rid_;
};

class RM_Manager {
 public:
  RM_Manager(PF_Manager &pfm);
  ~RM_Manager();

  Status CreateFile(const char *file_name, int record_size);
  Status DeleteFile(const char *file_name);
  Status OpenFile(const char *file_name, RM_FileHandle &file_handle);
  Status CloseFile(RM_FileHandle &file_handle);

 private:
  PF_Manager &pf_manager_;

  int CalcBitmapSize(int num_records);
  int CalcNumRecordsPerPage(int record_size);
};

}  // namespace toydb

#endif  // TOYDB_PF_H_
