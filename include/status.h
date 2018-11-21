/*
 * status.h
 *
 *  Created on: Mar 30, 2018
 *      Author: zyh
 */

#ifndef TOYDB_STATUS_H_
#define TOYDB_STATUS_H_

#include <string.h>
#include <iostream>

namespace toydb {

enum class ErrorCode {
  kOK = 0,
  kPF = 1,
  kRM = 2,
  kIX = 3,
  kSM = 4,
  kQL = 5,
  kWL = 6
};

struct ErrorInfo {
  ErrorCode error_code;
  std::string reason;

  ErrorInfo(ErrorCode code, const char *msg);
};

class Status {
 public:
  Status() : error_info_(nullptr) { }
  Status(ErrorCode code);
  Status(ErrorCode code, const char *reason);
  ~Status() { }

  Status(const Status &s);
  void operator=(const Status &s);
  bool operator==(const Status &s);

  static inline Status OK() { return Status(); }

  inline bool ok() const { return (error_info_ == nullptr); }

  ErrorInfo *error_info_;

 private:
  friend std::ostream& operator<<(std::ostream &os, const Status &s);


};

std::ostream& operator<<(std::ostream &os, const Status &s);

}  // namespace toydb

#endif // TOYDB_STATUS_H_
