include_directories(../include)

# test PF
add_executable(testpf test_pf.cc)
target_link_libraries(testpf pf utils)

# test RM
add_executable(testrm test_rm.cc)
target_link_libraries(testrm rm pf utils)

# test IX
add_executable(testix test_ix.cc)
target_link_libraries(testix ix rm pf utils)
