AM_CPPFLAGS = -I$(srcdir)/include
ACLOCAL_AMFLAGS = -I config
AM_CFLAGS = -g -Wall -Werror -I.

bin_PROGRAMS = examples/ibv_cc_pingpong examples/ibv_task_pingpong examples/ibv_cc_barrier tests/ibv_cc_test examples/cc2_send_pingpong examples/cc2_rdma_pingpong

examples_ibv_cc_pingpong_SOURCES = examples/cc_pingpong.c
examples_ibv_task_pingpong_SOURCES = examples/task_pingpong.c
examples_ibv_cc_barrier_SOURCES = examples/cc_main.c
examples_cc2_rdma_pingpong_SOURCES = examples/cc2_pingpong.c examples/cc2_pingpong.h
examples_cc2_send_pingpong_SOURCES = examples/cc2_pingpong.c examples/cc2_pingpong.h

examples_cc2_rdma_pingpong_CFLAGS = -DUSE_RDMA -g -Wall -O3 -fno-strict-aliasing

tests_ibv_cc_test_CXXFLAGS = -g -Wall -O3 -fno-strict-aliasing

tests_ibv_cc_test_CPPFLAGS = \
	-I$(top_srcdir)/tests \
	-I$(top_srcdir)/tests/common
tests_ibv_cc_test_SOURCES = \
	tests/common/gtest_cmn.h \
	tests/gtest/gtest.h \
	tests/api/gtest_cd.h \
	tests/gtest_main.cc \
	tests/common/gtest_cmn.cc \
	tests/api/gtest_cd_tc1.cc \
	tests/api/gtest_cd_tc2.cc \
	tests/api/gtest_cd_tc3.cc \
	tests/api/gtest_cd_tc4.cc \
	tests/api/gtest_cd_tc5.cc \
	tests/api/gtest_cd_tc6.cc \
	tests/api/gtest_cd_tc7.cc \
	tests/api/gtest_cd_tc8.cc \
	tests/api/gtest_init.cc

EXTRA_DIST = tests/gtest/gtest-all.cc \
    examples/pingpong.h examples/cc_pingpong.h

EXTRA_DIST += autogen.sh
