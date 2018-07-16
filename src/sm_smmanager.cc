/*
 * sm_smmanger.cc
 *
 *  Created on: May 31, 2018
 *      Author: zyh
 */

#include "sm.h"
#include "toydb.h"
#include <unistd.h>
#include <string.h>
#include <set>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>


namespace toydb {

SM_Manager::SM_Manager(IX_Manager &ixm, RM_Manager &rmm) : ixm_(ixm), rmm_(rmm) {

}

SM_Manager::~SM_Manager() {

}

Status SM_Manager::CreateDb(const char *db_name) {
  Status s;
  if (db_name == nullptr) {
    return Status(ErrorCode::kSM, "cannot create db. db name is null");
  }
  char buf[100];
  getcwd(buf,sizeof(buf));
  if (mkdir(db_name, 0755) < 0) {
    return Status(ErrorCode::kSM, "cannot create db dir");
  }
  chdir(db_name);
  s = rmm_.CreateFile("relcat", sizeof(RelEntry)); if (!s.ok()) return s;
  s = rmm_.CreateFile("attrcat", sizeof(AttrEntry)); if (!s.ok()) return s;
  chdir(buf);
  return Status::OK();
}

Status SM_Manager::OpenDb(const char *db_name) {
  Status s;
  if (db_name == nullptr) {
    return Status(ErrorCode::kSM, "cannot open db. db name is null");
  }
  if (chdir(db_name) < 0) {
    return Status(ErrorCode::kSM, "cannot open db.can not change dir to dbname");
  }
  s = rmm_.OpenFile("relcat", rel_cat_fh_); if (!s.ok()) return s;
  s = rmm_.OpenFile("attrcat", attr_cat_fh_); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::CloseDb() {
  Status s;
  s = rmm_.CloseFile(rel_cat_fh_); if (!s.ok()) return s;
  s = rmm_.CloseFile(attr_cat_fh_); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::CreateTable(const char *rel_name, int attr_count,
                               AttrInfo *attributes) {
  Status s;
  if (strlen(rel_name) > kMaxRelationName) {
    return Status(ErrorCode::kSM, "can not create table. relation name is too long");
  }
  if (attr_count < 1 || attr_count > kMaxAttrNum) {
    return Status(ErrorCode::kSM, "can not create table. attr count is invaid");
  }
  int record_size = 0;
  std::set<std::string> attr_names;
  for (int i = 0; i < attr_count; i++) {
    if (strlen(attributes[i].attrName) > kMaxAttrName) {
      return Status(ErrorCode::kSM, "can not create table. attr name is too long");
    }
    if (!isValidAttrType(attributes[i])) {
      return Status(ErrorCode::kSM, "can not create table. attr type is invalid");
    }
    std::string str(attributes[i].attrName);
    attr_names.insert(str);
    record_size += attributes[i].attrLength;
  }
  if (attr_names.size() != (unsigned int) attr_count) {
    return Status(ErrorCode::kSM, "can not create table. attr name is duplicated");
  }

  s = rmm_.CreateFile(rel_name, record_size); if (!s.ok()) return s;
  int offset = 0;
  for (int i = 0; i < attr_count; i++) {
    s = InsertIntoAttrCat(rel_name, attributes[i], offset, i); if (!s.ok()) return s;
    offset += attributes[i].attrLength;
  }
  s = InsertIntoRelCat(rel_name, attr_count, record_size); if (!s.ok()) return s;
  s = attr_cat_fh_.ForcePages(); if (!s.ok()) return s;
  s = rel_cat_fh_.ForcePages(); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::DropTable(const char *rel_name) {
  Status s;
  if (strlen(rel_name) > kMaxRelationName) {
    return Status(ErrorCode::kSM, "cannot drop table. relation name is too long");
  }
  s = rmm_.DeleteFile(rel_name); if (!s.ok()) return s;
  Record rel_rec;
  RelEntry *rel_entry;
  s = GetRelEntry(rel_name, rel_rec, rel_entry); if (!s.ok()) return s;
  RM_FileScan fs;
  s = fs.OpenScan(attr_cat_fh_, STRING, kMaxAttrName + 1, 0, EQ_OP, (void *) rel_name);
  if (!s.ok()) return s;
  for (int i = 0; i < rel_entry->attrCount; i++) {
    Record attr_rec;
    bool eof;
    s = fs.GetNextRec(attr_rec, eof); if (!s.ok()) return s;
    if (eof) return Status(ErrorCode::kSM, "attribute catalog file is broken");
    AttrEntry *entry = (AttrEntry *)attr_rec.data_;
    if (entry->indexNo != -1) {
      s = DropIndex(rel_name, entry->attrName);
    }
    attr_cat_fh_.DeleteRec(attr_rec.rid_); if (!s.ok()) return s;
  }
  s = fs.CloseScan(); if (!s.ok()) return s;
  s = rel_cat_fh_.DeleteRec(rel_rec.rid_); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::CreateIndex(const char *rel_name, const char *attr_name) {
  Status s;
  Record rel_rec;
  RelEntry *r_entry;
  s = GetRelEntry(rel_name, rel_rec, r_entry); if (!s.ok()) return s;

  Record attr_rec;
  AttrEntry *a_entry;
  s = GetAttrEntry(rel_name, attr_name, attr_rec, a_entry); if (!s.ok()) return s;
  if (a_entry->indexNo != -1) {
    return Status(ErrorCode::kSM, "cannot create this index. it already exists");
  }
  s = ixm_.CreateIndex(rel_name, r_entry->indexCurrNum, a_entry->attrType,
                   a_entry->attrLength);
  if (!s.ok()) return s;
  IX_IndexHandle ixh;
  RM_FileHandle fh;
  RM_FileScan fs;
  s = ixm_.OpenIndex(rel_name, r_entry->indexCurrNum, ixh); if (!s.ok()) return s;
  s = rmm_.OpenFile(rel_name, fh); if (!s.ok()) return s;
  s = fs.OpenScan(fh, INT, 4, 0, NO_OP, NULL); if (!s.ok()) return s;
  while(1) {
    Record rec;
    bool eof;
    s = fs.GetNextRec(rec, eof); if (!s.ok()) return s;
    if (eof) break;
    s = ixh.InsertEntry(rec.data_, rec.rid_); if (!s.ok()) return s;
  }
  s = fs.CloseScan(); if (!s.ok()) return s;
  s = rmm_.CloseFile(fh); if (!s.ok()) return s;
  s = ixm_.CloseIndex(ixh); if (!s.ok()) return s;
  a_entry->indexNo = r_entry->indexCurrNum;
  r_entry->indexCurrNum++;
  r_entry->indexCount++;
  s = rel_cat_fh_.UpdateRec(rel_rec); if (!s.ok()) return s;
  s = attr_cat_fh_.UpdateRec(attr_rec); if (!s.ok()) return s;
  s = rel_cat_fh_.ForcePages(); if (!s.ok()) return s;
  s = attr_cat_fh_.ForcePages(); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::DropIndex(const char *rel_name, const char *attr_name) {
  Status s;
  Record rel_rec;
  RelEntry *r_entry;
  s = GetRelEntry(rel_name, rel_rec, r_entry); if (!s.ok()) return s;

  Record attr_rec;
  AttrEntry *a_entry;
  s = GetAttrEntry(rel_name, attr_name, attr_rec, a_entry); if (!s.ok()) return s;
  if (a_entry->indexNo == -1) {
    return Status(ErrorCode::kSM, "cannot drop this index. it does not exists");
  }
  s = ixm_.DestroyIndex(rel_name, a_entry->indexNo);
  a_entry->indexNo = -1;
  r_entry->indexCount--;
  s = rel_cat_fh_.UpdateRec(rel_rec); if (!s.ok()) return s;
  s = attr_cat_fh_.UpdateRec(attr_rec); if (!s.ok()) return s;
  s = rel_cat_fh_.ForcePages(); if (!s.ok()) return s;
  s = attr_cat_fh_.ForcePages(); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::InsertIntoAttrCat(const char *rel_name, AttrInfo &attr, int offset, int attr_num) {
  Status s;
  AttrEntry entry;
  RID rid;
  memset((void*)&entry, 0, sizeof(AttrEntry));
  memcpy(entry.relName, rel_name, strlen(rel_name));
  memcpy(entry.attrName, attr.attrName, strlen(attr.attrName));
  entry.offset = offset;
  entry.attrType = attr.attrType;
  entry.indexNo = -1;
  entry.attrLength = attr.attrLength;
  s = attr_cat_fh_.InsertRec((char *)&entry,rid); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::InsertIntoRelCat(const char *rel_name,int attr_count, int record_size) {
  Status s;
  RelEntry entry;
  RID rid;
  memset((void*)&entry, 0, sizeof(RelEntry));
  memcpy(entry.relName, rel_name, strlen(rel_name));
  entry.tupleLength = record_size;
  entry.attrCount = attr_count;
  entry.indexCount = 0;
  entry.indexCurrNum = 0;
  entry.numTuples = 0;
  s = rel_cat_fh_.InsertRec((char *)&entry, rid); if (!s.ok()) return s;
  return Status::OK();
}

Status SM_Manager::GetRelEntry(const char *rel_name, Record &rec, RelEntry *&rel_entry) {
  Status s;
  RM_FileScan fs;
  s = fs.OpenScan(rel_cat_fh_, STRING, kMaxRelationName + 1, 0, EQ_OP, (void *)rel_name);
  if (!s.ok()) return s;
  bool eof;
  s = fs.GetNextRec(rec,eof); if (!s.ok()) return s;
  if (eof) {
    return Status(ErrorCode::kSM, "no such relation");
  }
  s = fs.CloseScan(); if (!s.ok()) return s;
  rel_entry = (RelEntry *) rec.data_;
  return Status::OK();
}

bool SM_Manager::isValidAttrType(const AttrInfo &attribute) const {
  switch (attribute.attrType) {
    case INT: return attribute.attrLength == 4;
    case FLOAT: return attribute.attrLength == 4;
    case STRING: return (attribute.attrLength > 0 && attribute.attrLength < kMaxStrLen);
    default: return false;
  }
}

Status SM_Manager::GetAttrEntry(const char *rel_name, const char *attr_name,
                                Record &attr_rec, AttrEntry *&a_entry) {
  Status s;
  RM_FileScan fs;
  s = fs.OpenScan(attr_cat_fh_, STRING, kMaxRelationName + 1, 0, EQ_OP, (void *)rel_name);
  if (!s.ok()) return s;
  bool eof;
  while(1) {
    s = fs.GetNextRec(attr_rec, eof); if (!s.ok()) return s;
    AttrEntry *entry = (AttrEntry *) attr_rec.data_;
    if (eof) {
      s = fs.CloseScan(); if (!s.ok()) return s;
      return Status(ErrorCode::kSM, "no such attr");
    }
    if (strncmp(entry->attrName, attr_name, kMaxAttrName + 1) == 0) {
      break;
    }
  }
  s = fs.CloseScan(); if (!s.ok()) return s;
  a_entry = (AttrEntry *) attr_rec.data_;
  return Status::OK();
}

Status SM_Manager::GetAttrForRel(
    RelEntry *rel_entry, AttrEntry *attr_entry,
    std::map<std::string, std::vector<std::string> > &attr_to_rels) {
  Status s;
  RM_FileScan fs;
  s = fs.OpenScan(attr_cat_fh_, STRING, kMaxRelationName + 1, 0, EQ_OP,
                  (void *) rel_entry->relName);
  if (!s.ok()) return s;
  bool eof;
  Record attr_rec;
  for (int i = 0; i < rel_entry->attrCount; i++) {
    s = fs.GetNextRec(attr_rec, eof); if (!s.ok()) return s;
    if (eof) {
      s = fs.CloseScan(); if (!s.ok()) return s;
      return Status(ErrorCode::kSM, "attr num is not equal to attr entry num");
    }
    memcpy(attr_entry + i, attr_rec.data_, sizeof(AttrEntry));

    AttrEntry *entry = attr_entry + i;
    std::string rel_string(entry->relName);
    std::string attr_string(entry->attrName);
    std::map<std::string, std::vector<std::string> >::iterator iter = attr_to_rels.find(attr_string);
    if (iter == attr_to_rels.end()) {
      std::vector<std::string> rels;
      rels.push_back(rel_string);
      attr_to_rels.insert({attr_string, rels});
    } else {
      iter->second.push_back(rel_string);
    }
  }
  s = fs.CloseScan(); if (!s.ok()) return s;
  return Status::OK();
}

}  // namespace toydb
