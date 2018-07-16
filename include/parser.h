//
// parser.h
//   Parser Component Interface
//

#ifndef PARSER_H
#define PARSER_H

#include <iostream>
#include "toydb.h"
#include "pf.h"
#include "sm.h"
#include "ql.h"


using namespace toydb;
//
// Parse function
//

void RBparse(PF_Manager &pfm, SM_Manager &smm, QL_Manager &qlm);

//
// Error printing function; calls component-specific functions
//
//void PrintError(RC rc);

// bQueryPlans is allocated by parse.y.  When bQueryPlans is 1 then the
// query plan chosen for the SFW query will be displayed.  When
// bQueryPlans is 0 then no query plan is shown.
extern int bQueryPlans;

#endif
