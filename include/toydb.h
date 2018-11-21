/*
 * toydb.h
 *
 *  Created on: Mar 30, 2018
 *      Author: zyh
 */

#ifndef TOYDB_TOYDB_H_
#define TOYDB_TOYDB_H_

void yyerror(const char *);
#define yywrap() 1
#define YY_SKIP_YYWRAP 1

#include <assert.h>

namespace toydb {

typedef int PageNum;

typedef int SlotNum;

typedef int RC;

static const int MAXATTRS = 40;
static const int MAXNAME = 24;
static const int MAXSTRINGLEN = 255;

enum AttrType {
  INT,
  FLOAT,
  STRING
};

static const int kMaxStrLen = 4096;

static const int kMaxRelationName = 24;

static const int kMaxAttrName = 24;

static const int kMaxAttrNum = 40;

static const int kMaxOutputString = 12;

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
