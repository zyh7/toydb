/*
 * test_pf.cc
 *
 *  Created on: Apr 22, 2018
 *      Author: zyh
 */

#include "pf.h"
#include <iostream>
#include <string.h>

using namespace toydb;
using namespace std;

void test_status() {
  cout << "testing status..." <<endl;
  Status s = Status(ErrorCode::kPF, "just a test");
  cout << s << endl;
}

void test_pfm() {
  cout << "testing pagefile manager..." << endl;
  Status s;
  PF_Manager pfm;
  PF_FileHandle fh;
  Page p;

  s = pfm.CreateFile("test.db");
  cout << "create file:" << s << endl;

  s = pfm.OpenFile("test.db", fh);
  cout << "open file:" << s << endl;
  cout << "writing file..." << endl;
  // write and force
  for (int i = 0; i < 50; i++) {
    cout << "allocate page:" << fh.AllocatePage(p) <<endl;
    sprintf(p.data, "this is the %dth page", i);
    cout << "write page:"<<p.data << endl;
    cout << "force page:" << fh.ForcePages(i) << endl;
    cout << "unpin page:" << fh.UnpinPage(i) << endl;
  }
  // write and flush
  for (int i = 50; i < 80; i++) {
    cout << "allocate page:" << fh.AllocatePage(p) <<endl;
    sprintf(p.data, "this is the %dth page", i);
    cout << "write page:" << p.data << endl;
    cout << "unpin page:" << fh.UnpinPage(i) << endl;
  }
  cout << "flush pages" << fh.FlushPages() <<endl;
  cout << "reading file..." << endl;

  for (int i = 0; i < 80; i++) {
    cout << "get page:" << fh.GetPage(i, p) << endl;
    cout << "read page:" << p.data << endl;
    cout << "unpin page" <<fh.UnpinPage(i) << endl;
  }

  s = pfm.CloseFile(fh);
  cout << "close file:" << s <<endl;
  s = pfm.DeleteFile("test.db");
  cout << "delete file:" << s <<endl;
}

int main() {
  test_status();
  test_pfm();
  return 0;
}
