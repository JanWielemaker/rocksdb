# Build rocksdb library for SWI-Prolog

# TODO: rocksdb	/Makefile has a lot of undefined variables
# MAKEFLAGS=--warn-undefined-variables

# For development, specify the following:
# ADDED_CPP_FLAGS= -Wsign-compare -Wshadow -Wunused-parameter -Woverloaded-virtual -Wnon-virtual-dtor -Wno-invalid-offsetof -Wconversion -Warith-conversion -Wsign-conversion -Wfloat-conversion -Wno-unused-parameter -Wno-missing-field-initializers

# For debugging:
#  -O0 -gdwarf-2 -g3 -fsanitize=address -fno-omit-frame-pointer

# To build/test:
#   cd $SRC/rocksdb
#   swipl pack install .
# and thereafter:
#   (cd $SRC/rocksdb && source buildenv.sh && make && make check)

# The following variables should be set by Make, but in case they're
# not, get the values that swipl sets (also in buildenv.sh)
CC ?= $(SWIPL_CC)
CXX ?= $(SWIPL_CXX)

# The following should be set by buildenv.sh:
SWIPL ?= swipl

CPPFLAGS=-Wall $(ADDED_CPPFLAGS) -std=c++17 -O2 -gdwarf-2 -g3 $(SWIPL_CFLAGS) $(SWIPL_MODULE_LDFLAGS) -Irocksdb/include
LIBROCKSDB=rocksdb/librocksdb.a
ROCKSENV=ROCKSDB_DISABLE_JEMALLOC=1 ROCKSDB_DISABLE_TCMALLOC=1
# DEBUG_LEVEL=0 implies -O2 without assertions and debug code
ROCKSCFLAGS=CXX=$(CXX) EXTRA_CXXFLAGS="-fPIC -Wall -O2 -gdwarf-2 -g3" EXTRA_CFLAGS="-fPIC -Wall -O2 -gdwarf-2 -g3" USE_RTTI=1 DEBUG_LEVEL=0
PLPATHS=-p library=prolog -p foreign="$(SWIPL_MODULE_DIR)"
SUBMODULE_UPDATE ?= git submodule update --init rocksdb

# sets PLATFORM_LDFLAGS
-include rocksdb/make_config.mk

all:	plugin

.PHONY: FORCE all clean install check distclean realclean shared_object plugin

rocksdb/INSTALL.md: FORCE
	$(SUBMODULE_UPDATE)

# Run the build for librocksdb in parallel, using # processors as
# limit, if using GNU make
JOBS=$(shell $(MAKE) --version 2>/dev/null | grep GNU >/dev/null && J=$$(nproc 2>/dev/null) && echo -j$$J)
$(LIBROCKSDB): rocksdb/INSTALL.md FORCE
	$(ROCKSENV) $(MAKE) $(JOBS) -C rocksdb static_lib $(ROCKSCFLAGS)

plugin:	$(LIBROCKSDB)
	$(MAKE) shared_object

shared_object: $(SWIPL_MODULE_DIR)/rocksdb4pl.$(SWIPL_MODULE_EXT)

$(SWIPL_MODULE_DIR)/rocksdb4pl.$(SWIPL_MODULE_EXT): cpp/rocksdb4pl.cpp $(LIBROCKSDB) Makefile
	mkdir -p $(SWIPL_MODULE_DIR)
	$(CXX) --version
	$(CXX) $(CPPFLAGS) -shared -o $@ cpp/rocksdb4pl.cpp $(LIBROCKSDB) $(PLATFORM_LDFLAGS) $(SWIPL_MODULE_LIB)

install::

check::
	@# TODO: determine which tests to run
	@# $(ROCKSENV) $(MAKE) $(JOBS) -C rocksdb tests

check::
	$(SWIPL) $(PLPATHS) -g test_rocksdb -t halt test/test_rocksdb.pl

distclean: clean
	$(RM) $(SWIPL_MODULE_DIR)/rocksdb4pl.$(SWIPL_MODULE_EXT)

clean:
	$(RM) *~

realclean: distclean
	git -C rocksdb clean -xfd
