/*
 * sm.h
 *
 *  Created on: May 31, 2018
 *      Author: zyh
 */

#ifndef TOYDB_IX_H
#define TOYDB_IX_H

#include <string>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include "toydb.h"
#include "rm.h"
#include "ix.h"

namespace toydb {

class WAL_Manager;

// Used by SM_Manager::CreateTable
struct AttrInfo {
  char *attrName;           // Attribute name
  AttrType attrType;            // Type of attribute
  int attrLength;          // Length of attribute
};

struct AttrEntry {
  char relName[kMaxRelationName + 1];  // Relation name
  char attrName[kMaxRelationName + 1];  // Attribute name
  int offset;              // Offset of attribute
  AttrType attrType;            // Type of attribute
  int attrLength;          // Length of attribute
  int indexNo;             // Attribute index number
};

struct RelEntry{
  char relName[kMaxRelationName + 1];
  int id;  // starting from 1
  int tupleLength;
  int attrCount;
  int indexCount;
  int indexCurrNum;
  int numTuples;
};

// Used by Printer class
struct DataAttrInfo {
  char relName[kMaxRelationName + 1];  // Relation name
  char attrName[kMaxRelationName + 1];  // Attribute name
  int offset;              // Offset of attribute
  AttrType attrType;            // Type of attribute
  int attrLength;          // Length of attribute
  int indexNo;             // Attribute index number
};

class SM_Manager {
  friend class QL_Manager;
 public:
  SM_Manager(IX_Manager &ixm, RM_Manager &rmm, WAL_Manager &wlm);
  ~SM_Manager();
  Status CreateDb(const char *db_name);
  Status OpenDb(const char *db_name);
  Status CloseDb();
  Status CreateTable(const char *rel_name, int attr_count, AttrInfo *attributes);
  Status DropTable(const char *rel_name);
  Status CreateIndex(const char *rel_name, const char *attr_name);
  Status DropIndex(const char *rel_name, const char *attr_name);
//  Status Load(const char *rel_name, const char *file_name);
//  Status Help();
//  Status Help(const char *rel_name);
//  Status Print(const char *rel_name);
//  Status Set(const char *param_name, const char *value);
 private:
  IX_Manager &ixm_;
  RM_Manager &rmm_;
  WAL_Manager &wlm_;

  RM_FileHandle rel_cat_fh_;
  RM_FileHandle attr_cat_fh_;
  bool isValidAttrType(const AttrInfo &attribute) const;
  Status InsertIntoAttrCat(const char *rel_name, AttrInfo &attr, int offset, int attr_num);
  Status InsertIntoRelCat(const char *rel_name,int attr_count, int record_size, int &id);
  Status GetRelEntry(const char *rel_name, Record &rec, RelEntry *&rel_entry);
  Status GetAttrEntry(const char *rel_name, const char *attr_name, Record &attr_rec,
                  AttrEntry *&a_entry);
  Status GetAttrForRel(RelEntry *rel_entry, AttrEntry *attr_entry,
                       std::map <std::string, std::vector <std::string> > &attr_to_rel);
};

}

#endif  // TOYDB_IX_H
