/*
 * test_parser.cc
 *
 *  Created on: Jul 9, 2018
 *      Author: zyh
 */

#include <iostream>
#include "parser.h"
#include "ix.h"
#include "pf.h"
#include "rm.h"
#include "sm.h"
#include "ql.h"
#include "wal.h"
#include "toydb.h"

using namespace toydb;
using namespace std;

int main() {
  WAL_Manager wlm;
  PF_Manager pfm;
  RM_Manager rmm(pfm);
  IX_Manager ixm(pfm);
  SM_Manager smm(ixm, rmm, wlm, pfm);
  QL_Manager qlm(smm, ixm, rmm);

  Status s;

  s = smm.CreateDb("testdb");
  if (!s.ok()) cout << s;


  s = smm.OpenDb("testdb");
  if (!s.ok()) cout << s;

  RBparse(pfm, smm, qlm);

  s = smm.CloseDb();
  if (!s.ok()) cout << s;

  return 0;
}
