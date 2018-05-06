include_directories(. ../include)
add_library(utils STATIC status.cc)
add_library(pf STATIC pf_pfmanager.cc pf_filehandle.cc pf_hashtable.cc
            pf_buffermanager.cc pf_hashtable.cc) 
add_library(rm STATIC rm_rmmanager.cc rm_filehandle.cc rm_filescan.cc 
            rm_record.cc rm_rid.cc)
