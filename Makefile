MODULE_big = bztree
OBJS = ./bztree_fdw.o \
./pmwcas/src/common/epoch.o \
./pmwcas/src/common/allocator_internal.o \
./pmwcas/src/common/pmwcas_internal.o \
./pmwcas/src/common/environment_internal.o \
./pmwcas/src/environment/environment_linux.o \
./pmwcas/src/environment/environment.o \
./pmwcas/src/mwcas/mwcas.o \
./pmwcas/src/util/nvram.o \
./pmwcas/src/util/doubly_linked_list.o \
./pmwcas/src/util/status.o \
./bztree.o

PGFILEDESC = "bztree - bztree implementation based on PMWCAS"

PG_CPPFLAGS += -std=c++14 -I./pmwcas -I./pmwcas/include -I./pmwcas/src
SHLIB_LINK   = -lstdc++ -lnuma

EXTENSION = bztree
DATA = bztree--1.0.sql

REGRESS = test
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/bztree/bztree.conf


ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bztree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
