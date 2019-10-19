# src/test/modules/test_pglz/Makefile

MODULE_big = test_pglz
OBJS = test_pglz.o pg_lzcompress_vanilla.o pg_lzcompress_hacked.o pg_lzcompress_hacked_compression.o $(WIN32RES)
PGFILEDESC = "test_pglz - test code for different pglz implementations"

EXTENSION = test_pglz
DATA = test_pglz--1.0.sql 000000010000000000000006 \
	000000010000000000000001 000000010000000000000008 16398 shakespeare.txt \
	mr dickens mozilla nci ooffice osdb reymont samba sao webster x-ray xml

# adversary5 adversary7 adversary_rnd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_pglz
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
