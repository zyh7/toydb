/*
 * rm_rid.cc
 *
 *  Created on: Apr 24, 2018
 *      Author: zyh
 */

#include "rm.h"

namespace toydb {

RID& RID::operator=(const RID &rid) {
  if (this != &rid) {
    page_num = rid.page_num;
    slot_num = rid.slot_num;
  }
  return *this;
}

bool RID::operator==(const RID &rid) const {
  return (page_num == rid.page_num && slot_num == rid.slot_num);
}

}


