/*
 * status.cc
 *
 *  Created on: Mar 30, 2018
 *      Author: zyh
 */

#include "status.h"

namespace toydb {

Status::Status(ErrorCode code) : Status(code, nullptr) { }

Status::Status(ErrorCode code, const char *reason) {
  if (code == ErrorCode::kOK) error_info_ = nullptr;
  else error_info_ = new ErrorInfo(code, reason);
}

Status::Status(const Status &s) {
  error_info_ = s.error_info_;
}

void Status::operator=(const Status &s) {
  error_info_ = s.error_info_;
}

ErrorInfo::ErrorInfo(ErrorCode code, const char *msg) {
  error_code = code;
  reason = msg;
}

std::ostream& operator<<(std::ostream &os, const Status &s) {
  if (s.error_info_ == nullptr) {
    os << "  --ok--  ";
  } else {
    os << "error part:"<< (int)s.error_info_->error_code << " reason:" << s.error_info_->reason;
  }
  return os;
}

}  // namespace toydb
