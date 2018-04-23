include_directories(. ../include)
add_library(utils STATIC status.cc)
add_library(pf STATIC pf_pfmanager.cc pf_filehandle.cc pf_hashtable.cc
            pf_buffermanager.cc pf_hashtable.cc) 
