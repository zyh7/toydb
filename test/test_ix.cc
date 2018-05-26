/*
 * test_ix.cc
 *
 *  Created on: May 25, 2018
 *      Author: zyh
 */

#include <iostream>
#include "ix.h"


using namespace toydb;
using namespace std;

Status test_ix() {
  Status s;
  PF_Manager pfm;
  IX_Manager ixm(pfm);
  IX_IndexHandle ixh;
  IX_IndexScan ixs;
  cout << "create index : " << ixm.CreateIndex("test.db", 0, INT, 4) << endl;
  cout << "open index : " << ixm.OpenIndex("test.db", 0, ixh) << endl;
  cout << "insert entry start ..." << endl;
  for (int i = 1; i < 50; i++) {
    for (int j = 1; j < 50; j++) {
      int value = i * j;
      RID rid(i, j);
      cout << i << " | " << j << endl;
      s = ixh.InsertEntry((void *) &value, rid); if (!s.ok()) return s;
    }
  }
  cout << "insert entry complete" << endl;
  cout << "delete entry start ..." << endl;
  for (int i = 1; i < 50; i++) {
    for (int j = 1; j < 50; j++) {
      if (j % 2) continue;
      int value = i * j;
      RID rid(i, j);
      cout << i << " | " << j << endl;
      s = ixh.DeleteEntry((void *) &value, rid); if (!s.ok()) return s;
    }
  }
  cout << "delete entry complete" << endl;
  int value = 50;
  cout << "openscan : " << ixs.OpenScan(ixh, GE_OP, (void *) &value) << endl;
  cout << "beginscan : " << ixs.BeginScan() << endl;
  bool eof = false;
  RID rid;
  while(1) {
    s = ixs.GetNextEntry(rid, eof); if (!s.ok()) return s;
    if (eof) break;
    cout << "page num : " << rid.page_num << " slot num : " << rid.slot_num << endl;
  }
  cout << "close scan : "  << ixs.CloseScan() << endl;
  cout << "force pages : " << ixh.ForcePages() << endl;
  cout << "delete scan : " << ixm.DestroyIndex("test.db", 0) << endl;
  return Status::OK();
}

int main() {
  Status s = test_ix();
  cout << "test index result : " <<s << endl;
}
