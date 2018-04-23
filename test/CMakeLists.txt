include_directories(../include)
add_executable(testpf test_pf.cc)
target_link_libraries(testpf utils pf)
