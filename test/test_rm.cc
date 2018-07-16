/*
 * test_rm.cc
 *
 *  Created on: Apr 29, 2018
 *      Author: zyh
 */

#include "rm.h"
#include "pf.h"
#include "status.h"
#include <iostream>
#include <string>
#include <vector>

using namespace toydb;
using namespace std;

// record: INT + FLOAT + STRING(64)
struct data {
  int a;
  float b;
  char c[64];
};

void test_rm() {
  PF_Manager pfm;
  RM_Manager rmm(pfm);
  RM_FileHandle rfh;
  cout << "create file: " <<rmm.CreateFile("test.db", 4 + 4 + 64) << endl;
  cout << "open file: " << rmm.OpenFile("test.db", rfh) << endl;
  vector<RID> rids;
  for (int i = 0; i < 150; i++) {
    data r;
    r.a = i;
    r.b = i/2.0;
    memset(r.c, 0, 64);
    string str = to_string(i);
    cout << str << endl;
    memcpy(r.c, str.data(), str.length());
    const char *data = (char *) &r;
    RID rid;
    cout << "insert rec: "<<rfh.InsertRec(data, rid) << endl;
    rids.push_back(rid);
  }

  RM_FileScan fs;
  int num = 75;
  bool eof = 0;
  void *value = (void *) &num;
  cout << "open scan" << fs.OpenScan(rfh, INT, 4, 0, LT_OP,value) << endl;
  while(1) {
    Record rec;
    cout << "get next record: " << fs.GetNextRec(rec, eof) << endl;
    if (eof == 1) break;
    data *r = (data *) rec.data_;
    cout << "int: " << r->a << " float: " << r->b << " string: " << r->c << endl;
  }

  for (unsigned int i = 0; i <  rids.size(); i++) {
    data *r;
    Record rec;
    cout << "get rec: "<< rfh.GetRec(rids[i], rec) << endl;
    r = (data *) rec.data_;
    cout << r->a << endl;
    cout << r->b << endl;
    cout << r->c << endl;
    r->a *= 2;
    r->b *= 2;
    cout << "update record: " << rfh.UpdateRec(rec) <<  endl;
    cout << "get rec: "<< rfh.GetRec(rids[i], rec) << endl;
    cout << r->a << endl;
    cout << r->b << endl;
    cout << r->c << endl;
    cout << "delete record: " << rfh.DeleteRec(rids[i]) << endl;

  }
  cout << "force pages: " << rfh.ForcePages() << endl;
  cout << "close file: " << rmm.CloseFile(rfh) << endl;
  cout << "delete file: " << rmm.DeleteFile("test.db") << endl;
}

int main() {
  test_rm();
  return 0;
}




