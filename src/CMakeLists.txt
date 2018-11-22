include_directories(. ../include)
add_library(utils STATIC status.cc)
add_library(wal STATIC wal_manager.cc wal_filehandle.cc wal_lock.cc)
add_library(pf STATIC pf_pfmanager.cc pf_filehandle.cc pf_hashtable.cc
            pf_buffermanager.cc pf_hashtable.cc pf_buffermanagerwal.cc
            pf_hashtablewal.cc) 
add_library(rm STATIC rm_rmmanager.cc rm_filehandle.cc rm_filescan.cc 
            rm_record.cc rm_rid.cc)
add_library(ix STATIC ix_ixmanager.cc ix_indexhandle.cc ix_indexscan.cc)
add_library(sm STATIC sm_smmanager.cc)
add_library(ql STATIC ql_node.cc ql_nodejoin.cc ql_nodeproj.cc ql_nodesel.cc
            ql_noderel.cc ql_qlmanager.cc)
add_library(parser STATIC parse.cc scan.cc nodes.cc interp.cc)
