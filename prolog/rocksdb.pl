/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2016-2022, VU University Amsterdam
			      SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(rocksdb,
	  [ rocks_open/3,		% +Directory, -RocksDB, +Options
	    rocks_close/1,		% +RocksDB
	    rocks_alias_lookup/2,	% +Name, -RocksDB

	    rocks_put/3,		% +RocksDB, +Key, +Value
	    rocks_put/4,		% +RocksDB, +Key, +Value, +Options
	    rocks_merge/3,		% +RocksDB, +Key, +Value
	    rocks_merge/4,		% +RocksDB, +Key, +Value, +Options
	    rocks_delete/2,		% +RocksDB, +Key
	    rocks_delete/3,		% +RocksDB, +Key, +Options
	    rocks_batch/2,		% +RocksDB, +Actions
	    rocks_batch/3,		% +RocksDB, +Actions, +Options

	    rocks_get/3,		% +RocksDB, +Key, -Value
	    rocks_get/4,		% +RocksDB, +Key, -Value, +Options
	    rocks_enum/3,		% +RocksDB, ?Key, ?Value
	    rocks_enum/4,		% +RocksDB, ?Key, ?Value, +Options
	    rocks_enum_from/4,		% +RocksDB, ?Key, ?Value, +From
	    rocks_enum_from/5,		% +RocksDB, ?Key, ?Value, +From, +Options
	    rocks_enum_prefix/4,	% +RocksDB, ?Suffix, ?Value, +Prefix
	    rocks_enum_prefix/5,	% +RocksDB, ?Suffix, ?Value, +Prefix, +Options

            rocks_property/2            % +RocksDB, ?Property
	  ]).
:- use_module(library(option)).
:- use_module(library(error)).
:- use_foreign_library(foreign(rocksdb4pl)).

:- meta_predicate
	rocks_open(+, -, :).

:- predicate_options(rocks_open/3, 3,
		     [ alias(atom),
		       mode(oneof([read_only,read_write])),
		       key(oneof([atom,string,binary,int32,int64,
				  float,double,term])),
		       value(any),
		       merge(callable),
                       debug(boolean),
                       prepare_for_bulk_load(oneof([true])),
                       optimize_for_small_db(oneof([true])),
                       increase_parallelism(oneof([true])),
                       create_if_missing(boolean),
                       create_missing_column_families(boolean),
                       error_if_exists(boolean),
                       paranoid_checks(boolean),
                       track_and_verify_wals_in_manifest(boolean),
                       info_log_level(oneof([debug,info,warn,error,fatal,header])), % default: info
                       env(boolean),
                       max_open_files(integer),
                       max_file_opening_threads(integer),
                       max_total_wal_size(integer),
                       statistics(boolean), % TODO: this only creates a Statistics object
                       use_fsync(boolean),
                       db_log_dir(string),
                       wal_dir(string),
                       delete_obsolete_files_period_micros(integer),
                       max_background_jobs(integer),
                       max_subcompactions(integer),
                       max_log_file_size(integer),
                       log_file_time_to_roll(integer),
                       keep_log_file_num(integer),
                       recycle_log_file_num(integer),
                       max_manifest_file_size(integer),
                       table_cache_numshardbits(integer),
                       wal_ttl_seconds(integer),
                       wal_size_limit_mb(integer),
                       manifest_preallocation_size(integer),
                       allow_mmap_reads(boolean),
                       allow_mmap_writes(boolean),
                       use_direct_reads(boolean),
                       use_direct_io_for_flush_and_compaction(boolean),
                       allow_fallocate(boolean),
                       is_fd_close_on_exec(boolean),
                       stats_dump_period_sec(integer),
                       stats_persist_period_sec(integer),
                       persist_stats_to_disk(boolean),
                       stats_history_buffer_size(integer),
                       advise_random_on_open(boolean),
                       db_write_buffer_size(integer),
                       write_buffer_manager(boolean),
                       % new_table_reader_for_compaction_inputs(boolean),  % TODO: removed from rocksdb/options.h?
                       compaction_readahead_size(integer),
                       random_access_max_buffer_size(integer),
                       writable_file_max_buffer_size(integer),
                       use_adaptive_mutex(boolean),
                       bytes_per_sync(integer),
                       wal_bytes_per_sync(integer),
                       strict_bytes_per_sync(integer),
                       enable_thread_tracking(boolean),
                       delayed_write_rate(integer),
                       enable_pipelined_write(boolean),
                       unordered_write(boolean),
                       allow_concurrent_memtable_write(boolean),
                       enable_write_thread_adaptive_yield(boolean),
                       max_write_batch_group_size_bytes(integer),
                       write_thread_max_yield_usec(integer),
                       write_thread_slow_yield_usec(integer),
                       skip_stats_update_on_db_open(boolean),
                       skip_checking_sst_file_sizes_on_db_open(boolean),
                       allow_2pc(boolean),
                       fail_ifoptions_file_error(boolean),
                       dump_malloc_stats(boolean),
                       avoid_flush_during_recovery(boolean),
                       avoid_flush_during_shutdown(boolean),
                       allow_ingest_behind(boolean),
                       % preserve_deletes(boolean), % TODO: removed: https://github.com/facebook/rocksdb/issues/9090
                       two_write_queues(boolean),
                       manual_wal_flush(boolean),
                       atomic_flush(boolean),
                       avoid_unnecessary_blocking_io(boolean),
                       write_dbid_to_manifest(boolean),
                       log_readahead_size(boolean),
                       best_efforts_recovery(boolean),
                       max_bgerror_resume_count(integer),
                       bgerror_resume_retry_interval(integer),
                       allow_data_in_errors(boolean),
                       db_host_id(string)
		     ]).
:- predicate_options(rocks_get/4, 4,
                     [
                      readahead_size(integer),
                      max_skippable_internal_keys(integer),
                      verify_checksums(boolean),
                      fill_cache(boolean),
                      tailing(boolean),
                      total_order_seek(boolean),
                      auto_prefix_mode(boolean),
                      prefix_same_as_start(boolean),
                      pin_data(boolean),
                      background_purge_on_iterator_cleanup(boolean),
                      ignore_range_deletions(boolean),
                      % iter_start_seqnum(integer), % TODO: removed https://github.com/facebook/rocksdb/issues/9090
                      io_timeout(integer),
                      value_size_soft_limit(integer)
                     ]).
:- predicate_options(rocks_enum/4, 4,
                     [ pass_to(rocks_get/4, 4)
                     ]).
:- predicate_options(rocks_enum_from/5, 5,
                     [ pass_to(rocks_get/4, 4)
                     ]).
:- predicate_options(rocks_enum_prefix/5, 5,
                     [ pass_to(rocks_get/4, 4)
                     ]).
:- predicate_options(rocks_put/4, 4,
                     [ sync(boolean),
                       disableWAL(boolean),
                       ignore_missing_column_families(boolean),
                       no_slowdown(boolean),
                       low_pri(boolean),
                       memtable_insert_hint_per_batch(boolean)
                     ]).
:- predicate_options(rocks_delete/3, 3,
                     [ pass_to(rocks_put/4, 4)
                     ]).
:- predicate_options(rocks_merge/4, 4,
                     [ pass_to(rocks_put/4, 4)
                     ]).
:- predicate_options(rocks_batch/4, 4,
                     [ pass_to(rocks_put/4, 4)
                     ]).

/** <module> RocksDB interface

RocksDB is an embeddable persistent key-value   store  for fast storage.
The store can be used only from one process  at the same time. It may be
used from multiple Prolog  threads  though.   This  library  provides  a
SWI-Prolog binding for RocksDB.  RocksDB   just  associates byte arrays.
This interface defines several mappings   between  Prolog datastructures
and byte arrays that may be configured   to  store both keys and values.
See rocks_open/3 for details.

@see http://rocksdb.org/
*/

%!	rocks_open(+Directory, -RocksDB, +Options) is det.
%
%	Open a RocksDB database in Directory and unify RocksDB with a
%	handle to the opened database. In general, this predicate
%	throws an exception on failure; if an error occurs in the
%	rocksdb library, the error term is of the form
%	rocks_error(Message) or rocks_error(Message,Blob).
%
%	Most of the `DBOptions` in `rocksdb/include/rocksdb/options.h`
%	are supported.  `create_if_exists` defaults to `true`.
%	Additional options are:
%
%	  - alias(+Name)
%	  Give the database a name instead of using an anonymous
%	  handle.  A named database is not subject to GC and must be
%	  closed explicitly. When the database is opened, RocksDB
%	  unifies with Name (the underlying handle can obtained using
%	  rocks_alias_lookup2).

%	  - key(+Type)
%	  - value(+Type)
%	  Define the type for the key and value. These must be
%	  consistent over multiple invocations. Default is `atom`.
%	  Defined types are:
%	    - atom
%	      Accepts an atom or string.  Unifies the result with an
%	      atom.  Data is stored as a UTF-8 string in RocksDB.
%	    - string
%	      Accepts an atom or string.  Unifies the result with a
%	      string.  Data is stored as a UTF-8 string in RocksDB.
%	    - binary
%	      Accepts an atom or string with codes in the range 0..255.
%	      Unifies the result with a string. Data is stored as a
%	      sequence of bytes in RocksDB.
%	    - int32
%	      Maps to a Prolog integer in the range
%	      -2,147,483,648...2,147,483,647.  Stored as a 4 bytes in
%	      native byte order.
%	    - int64
%	      Maps to a Prolog integer in the range
%	      -9223372036854775808..9223372036854775807 Stored as a 8
%	      bytes in native byte order.
%	    - float
%	      Value is mapped to a 32-bit floating point number.
%	    - double
%	      Value is mapped to a 64-bit floating point number (double).
%	    - term
%	      Stores any Prolog term. Stored using PL_record_external().
%	      The PL_record_external() function serializes the internal
%	      structure of a term, including _cycles_, _sharing_ and
%	      _attributes_.  This means that if the key is a term, it
%	      only matches if the the same cycles and sharing is
%	      used. For example, `X = f(a), Key = k(X,X)` is a different
%	      key from `Key = k(f(a),f(a))` and `X = [a|X]` is a
%	      different key from `X = [a,a|X]`. Applications for which
%	      such keys should match must first normalize the key.
%	      Normalization can be based on term_factorized/3 from
%	      library(terms).
%	  In addition, `value` accepts one of list(type) or set(type),
%	  currently only for the numeric types.  This causes
%	  rocks_put/3 and rocks_get/3 to exchange the value as a
%	  list and installs a built-in merge function.
%	  - merge(:Goal)
%	  Define RocksDB value merging.  See rocks_merge/3.
%	  - mode(+Mode)
%	  One of `read_write` (default) or `read_only`.  The latter
%	  uses OpenForReadOnly() to open the database. It is allowed
%	  to have multiple `read_only` opens, but only one
%	  `read_write` (which also precludes having any `read_only`);
%	  however, it is recommended to only open a databse once.
%         - debug(true) Output more information when displaying
%           the rocksdb "blob".
% @see https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
% @see http://rocksdb.org/blog/2018/08/01/rocksdb-tuning-advisor.html
% @see https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md

%
% @bug You must call rocks_close(Directory) to ensure clean shutdown
%	Failure to call rdb_close/1 usually doesn't result in data
%	loss because rocksdb can recover, depending on the setting of
%	the `sync` option. However, it is recommended that you do a
%	clean shutdown if possible, such as using at_halt/1 or
%	setup_call_cleanup/3 is used to ensure clean shutdown.

% @see https://github.com/facebook/rocksdb/wiki/Known-Issues

rocks_open(Dir, DB, Options0) :-
	meta_options(is_meta, Options0, Options),
        absolute_file_name(Dir, DirAbs),
	rocks_open_(DirAbs, DB, Options).

is_meta(merge).


%!	rocks_close(+RocksDB) is det.
%
%	Destroy the RocksDB handle.  Note that anonymous handles are
%	subject to (atom) garbage collection, which will call
%	rocks_close/1 as part of the garbage collection; however,
%	there is no guarantee that an anonymous handle will be garbage
%	collected, so it is suggested that at_halt/1 or
%	setup_call_cleanup/3 is used to ensure that rocks_close/1 is
%	called.
%
%	rocks_close/1 throws an existence error if RocksDB isn't a
%	valid handle or alias from rocks_open/3.  If RocksDB is an
%	anonymous handle that has been closed, rocks_close/1 silently
%	succeeds; if it's an alias name that's already been closed, an
%	existence error is raised (this behavior may change in
%	future).
%
%	If you call rocks_close/1 while there is an iterator open
%	(e.g., from rocks_enum/3 that still has a choicepoint), the
%	results are unpredicatable. The code attempts to avoid crashes
%	by reference counting iterators and only allowing a close if
%	there are no active iterators for a database.


%!	rocks_alias_lookup(+Name, -RocksDB) is semidet.
%
%	Look up an alias Name (as specified in rocks_open/3 `alias`
%	option and unify RocksDb with the underlying handle; fails if
%	there is no open file with the alias Name.
%
%	This predicate has two uses:
%	- The other predicates have slightly faster performance when the
%	  RocksDB handle is used instead of the Name.
%	- Some extra debugging information is available when the blob is printed.
%
%	Note that `rocks_open(...,RocksDB,[alias(Name)])` unifies
%	RocksDB with Name; if `alias(Name)` is not specified, RocksDB
%	is unified with the underlying handle.


%!	rocks_put(+RocksDB, +Key, +Value) is det.
%!      rocks_put(+RocksDB, +Key, +Value, Options) is det.
%
%	Add Key-Value to the RocksDB  database.   If  Key  already has a
%	value, the existing value is silently replaced by Value.  If the
%	value type is list(Type) or set(Type), Value must be a list. For
%	set(Type) the list is converted into an ordered set.

rocks_put(RocksDB, Key, Value) :-
    rocks_put(RocksDB, Key, Value, []).

%!	rocks_merge(+RocksDB, +Key, +Value) is det.
%!	rocks_merge(+RocksDB, +Key, +Value, +Options) is det.
%
%	Merge Value with the already existing   value  for Key. Requires
%	the option merge(:Merger) or the value type to be one of
%	list(Type) or set(Type) to be used when opening the database.
%	Using rocks_merge/3 rather than rocks_get/2, update and
%	rocks_put/3 makes the operation _atomic_ and reduces disk
%	accesses.
%
%       Options are the same as for rocks_put/4.
%
%	`Merger` is called as below, where two clauses are required:
%	one with `How` set to `partial` and one with `How` set to
%	`full`.  If `full`, `MergeValue` is a list of values that need
%	to be merged, if `partial`, `MergeValue` is a single value.
%
%	    call(:Merger, +How, +Key, +Value0, +MergeValue, -Value)
%
%	If Key is not in RocksDB, `Value0`  is unified with a value that
%	depends on the value type. If the value   type is an atom, it is
%	unified with the empty atom; if it is `string` or `binary` it is
%	unified with an empty string; if it  is `int32` or `int64` it is
%	unified with the integer 0; and finally if the type is `term` it
%	is unified with the empty list.
%
%	For example, if the value is a set  of Prolog values we open the
%	database with value(term) to allow for Prolog lists as value and
%	we define merge_set/5 as below.
%
%	==
%	merge(partial, _Key, Left, Right, Result) :-
%	    ord_union(Left, Right, Result).
%	merge(full, _Key, Initial, Additions, Result) :-
%	    append([Initial|Additions], List),
%	    sort(List, Result).
%	==
%
%	If the merge callback fails  or   raises  an exception the merge
%	operation fails and the error  is   logged  through  the RocksDB
%	logging facilities. Note that the merge   callback can be called
%	in a different thread or even in   a temporary created thread if
%	RocksDB decides to merge remaining values in the background.
%
%	@error	permission_error(merge, rocksdb RocksDB) if the database
%		was not opened with the merge(Merger) option.
%
%	@see https://github.com/facebook/rocksdb/wiki/Merge-Operator for
%	understanding the concept of value merging in RocksDB.

rocks_merge(RocksDB, Key, Value) :-
    rocks_merge(RocksDB, Key, Value, []).

%!	rocks_delete(+RocksDB, +Key) is semidet.
%!	rocks_delete(+RocksDB, +Key, +Options) is semidet.
%
%	Delete Key from RocksDB. Fails if Key is not in the database.
%
%       Options are the same as for rocks_put/4.

rocks_delete(RocksDB, Key) :-
    rocks_delete(RocksDB, Key, []).

%!	rocks_get(+RocksDB, +Key, -Value) is semidet.
%!      rocks_get(+RocksDB, +Key, -Value, +Options) is semidet.
%
%	True when Value is the current value associated with Key in
%	RocksDB.  If the value type is list(Type) or set(Type) this
%	returns a Prolog list.

rocks_get(RocksDB, Key, Value) :-
    rocks_get(RocksDB, Key, Value, []).

%!	rocks_enum(+RocksDB, -Key, -Value) is nondet.
%!	rocks_enum(+RocksDB, -Key, -Value, +Options) is nondet.
%
%	True when Value is the current value associated with Key in
%	RocksDB. This enumerates all keys in the database. If the value
%	type is list(Type) or set(Type) Value is a list.
%
%       Options are the same as for rocks_get/4.

rocks_enum(RocksDB, Key, Value) :-
    rocks_enum(RocksDB, Key, Value, []).

%!	rocks_enum_from(+RocksDB, -Key, -Value, +Prefix) is nondet.
%!	rocks_enum_from(+RocksDB, -Key, -Value, +Prefix, +Options) is nondet.
%
%	As rocks_enum/3, but starts  enumerating   from  Prefix. The key
%	type must be one of  `atom`,   `string`  or  `binary`. To _only_
%	iterate all keys with  Prefix,   use  rocks_enum_prefix/4 or the
%	construct below.
%
%       Options are the same as for rocks_get/4.
%
%	```
%	    rocks_enum_from(DB, Key, Value, Prefix),
%	    (   sub_atom(Key, 0, _, _, Prefix)
%	    ->  handle(Key, Value)
%	    ;   !, fail
%	    )
%	```

rocks_enum_from(RocksDB, Key, Value, Prefix) :-
    rocks_enum_from(RocksDB, Key, Value, Prefix, []).

%!	rocks_enum_prefix(+RocksDB, -Suffix, -Value, +Prefix) is nondet.
%!	rocks_enum_prefix(+RocksDB, -Suffix, -Value, +Prefix, +Options) is nondet.
%
%	True for all keys that start   with Prefix. Instead of returning
%	the full key this predicate returns the _suffix_ of the matching
%	key. This predicate  succeeds  deterministically if no next  key
%	exists or the next key does not match Prefix.
%
%       Options are the same as for rocks_get/4.

rocks_enum_prefix(RocksDB, Suffix, Value, Prefix) :-
    rocks_enum_prefix(RocksDB, Suffix, Value, Prefix, []).


%!	rocks_batch(+RocksDB, +Actions:list) is det.
%!	rocks_batch(+RocksDB, +Actions:list, +Options) is det.
%
%	Perform  a  batch  of  operations  on  RocksDB  as  an  _atomic_
%	operation.
%
%       Options are the same as for rocks_put/4.
%
%       Actions is a list of:
%
%	  - delete(+Key)
%	  As rocks_delete/2.
%	  - put(+Key, +Value)
%	  As rocks_put/3.
%
%	The  following  example  is   translated    from   the   RocksDB
%	documentation:
%
%	==
%	  rocks_get(RocksDB, key1, Value),
%	  rocks_batch(RocksDB,
%		      [ delete(key1),
%		        put(key2, Value)
%		      ])
%	==

rocks_batch(RocksDB, Actions) :-
    rocks_batch(RocksDB, Actions, []).


%!  rocks_property(+RocksDB, ?Property) is nondet

rocks_property(RocksDB, Property) :-
    var(Property), !,
    rocks_property(P),
    rocks_property(RocksDB, P, Value),
    Property =.. [P,Value].
rocks_property(RocksDB, Property) :-
    Property =.. [P,Value], !,
    rocks_property(RocksDB, P, Value).
rocks_property(_RocksDB, Property) :-
    type_error(property, Property).

rocks_property(estimate_num_keys).
