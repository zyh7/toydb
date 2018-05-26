/*
 * toydb.h
 *
 *  Created on: Mar 30, 2018
 *      Author: zyh
 */

#ifndef TOYDB_TOYDB_H_
#define TOYDB_TOYDB_H_

namespace toydb {

typedef int PageNum;

typedef int SlotNum;

enum AttrType {
  INT,
  FLOAT,
  STRING
};

static const int kMaxStrLen = 4096;

enum CompOp {
  NO_OP,
  EQ_OP,
  NE_OP,
  LT_OP,
  GT_OP,
  LE_OP,
  GE_OP
};

}  // namespace toydb

#endif // TOYDB_TOYDB_H_
