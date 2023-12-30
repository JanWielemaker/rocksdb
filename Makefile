#COFLAGS=-gdwarf-2 -g3

# For development, specify the following:
# ADDED_CPP_FLAGS= -Wsign-compare -Wshadow -Wunused-parameter -Woverloaded-virtual -Wnon-virtual-dtor -Wno-invalid-offsetof -Wconversion -Warith-conversion -Wsign-conversion -Wfloat-conversion -Wno-unused-parameter -Wno-missing-field-initializers

CPPFLAGS=-Wall $(ADDED_CPPFLAGS) -std=c++17 -O2 $(SWIPL_CFLAGS) $(COFLAGS) $(SWIPL_MODULE_LDFLAGS) -Irocksdb/include
LIBROCKSDB=rocksdb/librocksdb.a
ROCKSENV=ROCKSDB_DISABLE_JEMALLOC=1 ROCKSDB_DISABLE_TCMALLOC=1
# DEBUG_LEVEL=0 implies -O2 without assertions and debug code
ROCKSCFLAGS=EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0
PLPATHS=-p library=prolog -p foreign="$(SWIPL_MODULE_DIR)"
SWIPL ?= swipl
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
rocksdb/librocksdb.a: rocksdb/INSTALL.md FORCE
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
	rm -f $(SWIPL_MODULE_DIR)/rocksdb4pl.$(SWIPL_MODULE_EXT)

clean:
	rm -f *~

realclean: distclean
	git -C rocksdb clean -xfd
