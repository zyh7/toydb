/*
 * rm_record.cc
 *
 *  Created on: Apr 26, 2018
 *      Author: zyh
 */

#include "rm.h"
#include "string.h"
#include <stdlib.h>

namespace toydb {

Record::~Record() {
  if (data_ != nullptr) free(data_);
}

void Record::SetRecord(const RID &rid, const char *data, int size) {
  rid_ = rid;
  data_ = (char *) malloc(size);
  memcpy(data_, data, size);
}

}
