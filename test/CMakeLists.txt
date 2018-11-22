include_directories(../include)

# test PF
add_executable(testpf test_pf.cc)
target_link_libraries(testpf pf wal utils)

# test RM
add_executable(testrm test_rm.cc)
target_link_libraries(testrm rm pf wal utils)

# test IX
add_executable(testix test_ix.cc)
target_link_libraries(testix ix rm pf wal utils)

# test PARSER
add_executable(testparser test_parser.cc)
target_link_libraries(testparser parser ql sm ix rm pf wal utils -ltcmalloc_debug)
