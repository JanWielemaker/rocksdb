# SWI-Prolog interface for RocksDB

This is a SWI-Prolog pack that   provides library(rocksdb), a binding to
[RocksDB](http://rocksdb.org/).

## Installation

The installation requires a recent C++   compiler. RocksDB can work with
several compression libraries. Most  systems   have  zlib installed, but
others may provide better  performance  or   less  resource  usage.  See
[INSTALL.md](https://github.com/facebook/rocksdb/blob/master/INSTALL.md).
On Ubuntu 22.04 the following packages were installed:

    sudo apt install libsnappy-dev liblz4-dev libzstd-dev libgflags-dev

Once these are in place, a simple

    ?- pack_install(rocksdb).

Should do the trick. Note that this clones RocksDB and builds it the way
we need the library. This requires   significant  disk space (1.4Gb) and
takes long (several minutes on a modern machine).

This code depends on version 2 of the `SWI-cpp.h` file that is part of
SWI-Prolog version 8.5.16 and later.

#### Why are you not using the pre-built librocksdb?

There are a  number  of  issues   with  several  pre-built  versions  of
librocksdb:

  - Shared objects are often linked to jemalloc or tcmalloc. This
    prevents lazy loading of the library, causing either problems
    loading or running the embedded rocksdb.
  - Various libraries are compiled without RTTI (RunTime Type Info),
    which breaks subclassing RocksDB classes.
  - Static library is by default compiled without -fPIC and thus not
    usable.

As is, the most reliable way around  is   to  include RocksDB, so we can
control the version and build it the way  that best fits our needs: as a
static library with RTTI and -fPIC.


### Manual installation

If the above fails

  - Clone this prepository in the `pack` directory of your installation
    or clone it elsewhere and link it.
  - Run `?- pack_rebuild(rocksdb).` to rebuild it.  On failure, adjust
    `Makefile` to suit your installation and re-run the pack_rebuild/1
    command.

### Status

The wrapper provides most of functionality of RocksDB.  Column
families are not supported, nor are the following features
(this is not an exhaustive list):
   - user-defined timestamp
   - snapshots
   - slices
   - table filter
   - rate limiter
   - DB paths
   - event listeners
   - user-defined cache
   - user-defined checksums
   - user-defined statistics handler
   - user-defined logger
   - loading options from a file

### Options, logging and statistics

The options are defined and documented in file
[`rocksdb/include/rocksdb/options.h`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h).

RocksDB persists its options to an `OPTIONS` file but there currently
is no Prolog support loading this, as in
`rocksdb/examples/options_file_example.cc`.

RocksDB provides extensive logging and statistics for adjusting
the options. These are normally written to the `LOG` file(s) in
the database directory, but can be written elsewhere by setting
the `db_log_dir` option.

Statistics can be turned on by the `statistics(true)` option.
Additional statistics-related options are:
   - `stats_dump_period_sec`
   - `stats_persist_period_sec`
   - `persist_stats_to_disk`
   - `stats_history_buffer_size`

See also
https://github.com/facebook/rocksdb/wiki/Statistics
http://rocksdb.org/blog/2018/08/01/rocksdb-tuning-advisor.html
https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md

### Implementation notes

If you want to support user-defined statsitics callbacks
(`DBOptions.info_log`), it appears that there are multi-threaded
issues -- a simple implementation that called `Sfprintf(Suser_error,
...)` git a SEGV error when `Flush()` was implemented using
`Sflush(Suser_error)`. For reason reason, we don't support
`info_log(:Goal)` or `info_log_flush(:Goal)' in the optins.  Setting
`info_log_level results in LOG files in the RocksDB directory, which
is probably good enough.

Other callback implementations (such as `rate_limiter`, `statistics`,
`listeners`) could have similar problems.

### TODO

These seem to be the most useful missing features that don't require a lot of work:
   - `EventListener` callback, if there aren't multithreading issues.
   - Loading options from a file.
     - Possibly also loading the Prolog-only options, such as `key` and `value`.
   - User defined timestamps https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp-%28Experimental%29

