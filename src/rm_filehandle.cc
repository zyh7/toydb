/*
 * rm_filehandle.cc
 *
 *  Created on: Apr 24, 2018
 *      Author: zyh
 */

#include "rm.h"
#include <string.h>

namespace toydb {

RM_FileHandle::RM_FileHandle() {
  file_open_ = false;
}

RM_FileHandle::~RM_FileHandle() {

}

bool RM_FileHandle::IsValidRID(const RID &rid) const {
  return (rid.page_num > 0 && rid.page_num <= header_.num_pages
      && rid.slot_num >= 0 && rid.slot_num < header_.num_record_per_page);
}

Status RM_FileHandle::GetPageHeaderAndBitmap(const Page &p,
                                             RM_PageHeader *&header,
                                             char *&bitmap) const {
  if (p.data == nullptr) {
    return Status(ErrorCode::kRM,
                  "can not get bitmap and header. page is invalid");
  }
  header = (RM_PageHeader *) p.data;
  bitmap = p.data + header_.bitmap_offset;
  return Status::OK();
}

void RM_FileHandle::SetBit(char *bitmap, int bit_num) {
  int chunk = bit_num / 8;
  int offset = bit_num - chunk * 8;
  bitmap[chunk] |= (1 << offset);
}

void RM_FileHandle::ResetBit(char *bitmap, int bit_num) {
  int chunk = bit_num / 8;
  int offset = bit_num - chunk * 8;
  bitmap[chunk] &= ~(1 << offset);
}

bool RM_FileHandle::GetBit(const char *bitmap, int bit_num) const {
  int chunk = bit_num / 8;
  int offset = bit_num - chunk * 8;
  return bitmap[chunk] & (1 << offset);
}

bool RM_FileHandle::GetNextOneBit(const char *bitmap, int size,int &bit_num) const {
  for (int i = bit_num; i < size; i++) {
    int chunk = i / 8;
    int offset = i - chunk * 8;
    if (bitmap[chunk] & (1 << offset)) {
      bit_num = i;
      return true;
    }
  }
  return false;
}

Status RM_FileHandle::FindFirstZeroBit(const char *bitmap, int &slot_num) {
   for (int i = 0; i < header_.num_record_per_page; i++) {
     int chunk = i / 8;
     int offset = i - chunk * 8;
     if ((bitmap[chunk] & (1 << offset)) == 0) {
       slot_num =  i;
       return Status::OK();
     }
   }
   return Status(ErrorCode::kRM, "bitmap is full");
}

Status RM_FileHandle::AllocateNewPage(Page &p) {
  Status s;
  s = pfh_.AllocatePage(p); if (!s.ok()) return s;
  RM_PageHeader *page_header;
  char *bitmap;
  GetPageHeaderAndBitmap(p, page_header, bitmap);
  page_header->next_free = header_.next_free_page;
  page_header->num_records = 0;
  header_.next_free_page = p.num;
  header_.num_pages++;
  header_changed_ = true;
  memset(bitmap, 0, header_.bitmap_size);
  return Status::OK();
}

Status RM_FileHandle::InsertRec(const char *pData, RID &rid) {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kRM, "can not insert record. file is not open.");
  }
  Page p;
  if (header_.next_free_page == -1) {
    s = AllocateNewPage(p); if (!s.ok()) return s;
  } else {
    s = pfh_.GetPage(header_.next_free_page, p); if (!s.ok()) return s;
  }
  char *bitmap;
  RM_PageHeader *page_header;
  int slot_num;
  s = GetPageHeaderAndBitmap(p, page_header, bitmap); if (!s.ok()) return s;
  s = FindFirstZeroBit(bitmap, slot_num);
  SetBit(bitmap, slot_num);
  memcpy(bitmap + header_.bitmap_size + slot_num * header_.record_size,
         pData, header_.record_size);
  page_header->num_records++;
  if (page_header->num_records == header_.num_record_per_page) {
    header_.next_free_page = page_header->next_free;
    page_header->next_free = -2;
    header_changed_ = true;
  }
  s = pfh_.MarkDirty(p.num); if (!s.ok()) return s;
  s = pfh_.UnpinPage(p.num); if (!s.ok()) return s;
  rid.page_num = p.num;
  rid.slot_num = slot_num;
  return Status::OK();
}

Status RM_FileHandle::GetRec(const RID &rid, Record &rec) const {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kRM, "can not get record. file is not open");
  }
  if (!IsValidRID(rid)){
    return Status(ErrorCode::kRM, "can not get record. rid is invalid");
  }
  Page p;
  s = pfh_.GetPage(rid.page_num, p); if (!s.ok()) return s;
  char *bitmap;
  RM_PageHeader *page_header;
  s = GetPageHeaderAndBitmap(p, page_header, bitmap); if (!s.ok()) return s;
  if (!GetBit(bitmap, rid.slot_num)) {
    return Status(ErrorCode::kRM, "bit is 0. slot is empty");
  }
  rec.SetRecord(rid,
                bitmap + header_.bitmap_size +rid.slot_num * header_.record_size,
                header_.record_size);
  s = pfh_.UnpinPage(rid.page_num);
  return Status::OK();
}

Status RM_FileHandle::GetNextRec(RID &rid, Record &rec, bool &eof) const {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kRM, "can not get record. file is not open");
  }
  if (!IsValidRID(rid)) {
    return Status(ErrorCode::kRM, "can not get get next record. rid is invalid");
  }
  Page p;
  PageNum page_num = rid.page_num;
  int slot_num = rid.slot_num;

  char *bitmap;
  RM_PageHeader *page_header;
  while(1) {
    s = pfh_.GetNextPage(page_num, p, eof); if (!s.ok()) return s;
    if (eof == true) break;
    s = GetPageHeaderAndBitmap(p, page_header, bitmap); if (!s.ok()) return s;
    if (page_header->num_records > 0) {
      bool exist = GetNextOneBit(bitmap, page_header->num_records, slot_num);
      if (exist) {
        rid.page_num = page_num;
        rid.slot_num = slot_num;
        rec.SetRecord(
            rid, bitmap + header_.bitmap_size + slot_num * header_.record_size,
            header_.record_size);
        s = pfh_.UnpinPage(page_num);
        break;
      }
    }
    s = pfh_.UnpinPage(page_num);
    page_num++;
    slot_num = 0;
  }
  return Status::OK();
}

Status RM_FileHandle::DeleteRec(const RID &rid) {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kRM, "can not get record. file is not open");
  }
  if (!IsValidRID(rid)) {
    return Status(ErrorCode::kRM, "can not delete record. file is not open");
  }
  Page p;
  s = pfh_.GetPage(rid.page_num, p); if (!s.ok()) return s;
  char *bitmap;
  RM_PageHeader *page_header;
  s = GetPageHeaderAndBitmap(p, page_header, bitmap); if (!s.ok()) return s;
  if (!GetBit(bitmap, rid.slot_num)) {
    return Status(ErrorCode::kRM, "slot is empty.can not delete record");
  }
  ResetBit(bitmap, rid.slot_num);
  --(page_header->num_records);
  if (page_header->num_records == header_.next_free_page - 1) {
    page_header->next_free = header_.next_free_page;
    header_.next_free_page = rid.page_num;
    header_changed_ = true;
  }
  s = pfh_.MarkDirty(rid.page_num);
  s = pfh_.UnpinPage(rid.page_num);
  return Status::OK();
}

Status RM_FileHandle::UpdateRec(const Record &rec) {
  Status s;
  if (file_open_ == false) {
    return Status(ErrorCode::kRM, "can not get record. file is not open");
  }
  if (!IsValidRID(rec.rid_)) {
    return Status(ErrorCode::kRM, "can not update record. rid is invalid");
  }
  if (rec.data_ == nullptr) {
    return Status(ErrorCode::kRM, "can not update record. data is null");
  }
  PageNum page_num = rec.rid_.page_num;
  int slot_num = rec.rid_.slot_num;
  Page p;
  s = pfh_.GetPage(page_num, p); if (!s.ok()) return s;
  char *bitmap;
  RM_PageHeader *page_header;
  s = GetPageHeaderAndBitmap(p, page_header, bitmap); if (!s.ok()) return s;
  if (!GetBit(bitmap, slot_num)) {
    return Status(ErrorCode::kRM, "slot is empty.can not update record");
  }
  char *target = bitmap + header_.bitmap_size
      + slot_num * header_.record_size;
  memcpy(target, rec.data_, header_.record_size);
  s = pfh_.MarkDirty(page_num); if (!s.ok()) return s;
  s = pfh_.UnpinPage(page_num); if (!s.ok()) return s;
  return Status::OK();
}

Status RM_FileHandle::ForcePages(PageNum page_num) {
  Status s;
  if (page_num < -1 || page_num > header_.num_pages) {
    return Status(ErrorCode::kRM, "can not force page. page num is invalid");
  }
  s = pfh_.ForcePages(page_num);
  return s;
}

}




