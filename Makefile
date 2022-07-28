#COFLAGS=-gdwarf-2 -g3
# The following flags are the same as rocksdb uses, except that rocksdb also has -Werror
# and doesn't have the conversion flags in ADDED_CPPFLAGS
# TODO: Add -Werror
#       Unfortunately, there's noise from rocksdb headers
ADDED_CPPFLAGS=-Wconversion -Warith-conversion -Wsign-conversion -Wfloat-conversion -Wno-unused-parameter
CPPFLAGS=-Wall -Wextra -Wsign-compare -Wshadow -Wunused-parameter -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-invalid-offsetof $(ADDED_CPPFLAGS) -std=c++17 -O2 $(CFLAGS) $(COFLAGS) $(LDSOFLAGS) -Irocksdb/include
LIBROCKSDB=rocksdb/librocksdb.a
ROCKSENV=ROCKSDB_DISABLE_JEMALLOC=1 ROCKSDB_DISABLE_TCMALLOC=1
# DEBUG_LEVEL=0 implies -O2 without assertions and debug code
ROCKSCFLAGS=EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0
PLPATHS=-p library=prolog -p foreign="$(PACKSODIR)"

# sets PLATFORM_LDFLAGS
-include rocksdb/make_config.mk

all:	plugin

.PHONY: FORCE all clean install check distclean realclean shared_object plugin

rocksdb/INSTALL.md: FORCE
	git submodule update --init rocksdb

# Run the build for librocksdb in parallel, using # processors as
# limit, if using GNU make
JOBS=$(shell $(MAKE) --version 2>/dev/null | grep GNU >/dev/null && J=$$(nproc 2>/dev/null) && echo -j$$J)
rocksdb/librocksdb.a: rocksdb/INSTALL.md FORCE
	$(ROCKSENV) $(MAKE) $(JOBS) -C rocksdb static_lib $(ROCKSCFLAGS)

plugin:	$(LIBROCKSDB)
	$(MAKE) shared_object

shared_object: $(PACKSODIR)/rocksdb4pl.$(SOEXT)

$(PACKSODIR)/rocksdb4pl.$(SOEXT): cpp/rocksdb4pl.cpp $(LIBROCKSDB) Makefile
	mkdir -p $(PACKSODIR)
	$(CXX) --version
	@# For development:
	@#   PACKSODIR=/tmp/rocksdb-so
	@#   SOEXT=so
	@#   CFLAGS='-fPIC -pthread -I$$HOME/src/swipl-devel/src -I$$HOME/src/swipl-devel/src/os -I$$HOME/src/swipl-devel/packages/cpp'
	@# other flags: CPPFLAGS $(CPPFLAGS) LIBROCKSDB $(LIBROCKSDB) PLATFORM_LDFLAGS $(PLATFORM_LDFLAGS)
	@#              SWISOLIB $(SWISOLIB) CFLAGS$(CFLAGS) COFLAGS $(COFLAGS) LDSOFLAGS$(LDSOFLAGS)
	$(CXX) $(CPPFLAGS) -shared -o $@ cpp/rocksdb4pl.cpp $(LIBROCKSDB) $(PLATFORM_LDFLAGS) $(SWISOLIB)

install::

check::
	swipl $(PLPATHS) -g test_rocksdb -t halt test/test_rocksdb.pl

distclean: clean
	rm -f $(PACKSODIR)/rocksdb4pl.$(SOEXT)

clean:
	rm -f *~

realclean: distclean
	git -C rocksdb clean -xfd
