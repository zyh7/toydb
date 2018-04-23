/*
 * pf_internal.h
 *
 *  Created on: Apr 8, 2018
 *      Author: zyh
 */

#ifndef TOYDB_SRC_PF_INTERNAL_H_
#define TOYDB_SRC_PF_INTERNAL_H_

namespace toydb {

static const int kBufferSize = 40;
static const int kPageSize = 4096;
static const int kNumBuckets = 20;
static const int kFileHeaderSize = 4096;

struct PageHdr {
    int nextFree;       // nextFree can be any of these values:
                        //  - the number of the next free page
                        //  -1 if this is last free page
                        //  -2 if the page is not free
};

}

#endif  // TOYDB_SRC_PF_INTERNAL_H_
