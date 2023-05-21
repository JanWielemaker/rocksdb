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

:- module(test_rocksdb,
	  [ test_rocksdb/0
	  ]).
:- encoding(utf8).

:- asserta(user:file_search_path(foreign, '.')).
:- asserta(user:file_search_path(foreign, '..')).
:- asserta(user:file_search_path(foreign, '../cpp')).
:- asserta(user:file_search_path(library, '.')).
:- asserta(user:file_search_path(library, '../prolog')).

:- use_module(library(rocksdb)).
:- use_module(library(plunit)).
:- use_module(library(debug)).

test_rocksdb :-
	run_tests([ rocks,
		    terms,
		    types,
		    merge,
		    builtin_merge,
		    properties,
		    enum,
                    alias
		  ]).

:- begin_tests(rocks, [cleanup(delete_db)]).

test(basic, [Noot == noot,
	     setup(setup_db(Dir, RocksDB)),
	     cleanup(cleanup_db(Dir, RocksDB))]) :-
	rocks_put(RocksDB, aap, noot),
	rocks_get(RocksDB, aap, Noot),
	rocks_delete(RocksDB, aap),
	assertion(\+ rocks_get(RocksDB, aap, _)).

test(basic, [Vs_get_expect == Vs_get_actual,
	     setup(setup_db(Dir, RocksDB, [key(atom), value(term)])),
	     cleanup(cleanup_db(Dir, RocksDB))]) :-
	% TODO: binary
	KVs = [atom    - one,
	       string  - "two",
	       int32   - 2_147_483_647,
	       int64   - -9_223_372_036_854_775_808,
	       float32 - 1.5,
	       float64 - 3.14159,
	       term    - functor(1,2.0,three,"four")],
	random_permutation(KVs, KVs_put),
	random_permutation(KVs, KVs_get),
	random_permutation(KVs, KVs_delete),
	pairs_keys_values(KVs_put, Ks_put, Vs_put),
	pairs_keys_values(KVs_get, Ks_get, Vs_get_expect),
	pairs_keys(KVs_delete, Ks_delete),
	maplist(rocks_put(RocksDB), Ks_put, Vs_put),
	maplist(rocks_get(RocksDB), Ks_get, Vs_get_actual),
	maplist(rocks_delete(RocksDB), Ks_delete),
	maplist(assertion_key_not_found(RocksDB), Ks_delete).

assertion_key_not_found(RocksDB, Key) :-
	assertion(\+ rocks_get(RocksDB, Key, _)).

test(basic, [Noot == noot,
	     setup(setup_db(Dir, RocksDB)),
	     cleanup(cleanup_db(Dir, RocksDB))]) :-
	rocks_put(RocksDB, aap, noot, [sync(true)]),
	rocks_close(RocksDB),
	rocks_open(Dir, RocksDB2, [mode(read_only)]),
	rocks_get(RocksDB2, aap, Noot, [fill_cache(false)]).

test(options1, [error(type_error(bool,xxx),_),
		cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, _RocksDB, [error_if_exists(xxx)]).
test(options2, [error(type_error(option,this_is_not_an_option(123)),_),
		cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, _RocksDB, [this_is_not_an_option(123)]).
test(options3, [error(domain_error(mode_option,not_read_write),_),
		cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, _RocksDB, [mode(not_read_write)]).
test(options4, [error(rocks_error('Not implemented: Not supported operation in read only mode.'),_),
		cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, RocksDB0, [key(string), value(string)]),
	rocks_close(RocksDB0),
	setup_call_cleanup(
	    rocks_open(Dir, RocksDB, [mode(read_only)]),
	    (   rocks_put(RocksDB, "one", "àmímé níshíkíhéꜜbì"),
	        rocks_get(RocksDB, "one", "àmímé níshíkíhéꜜbì")
	    ),
	    rocks_close(RocksDB)).
test(options5, [cleanup(delete_db)]) :-
	test_db(Dir),
	setup_call_cleanup(
	    rocks_open(Dir, RocksDB, [key(string), value(string), mode(read_write)]),
	    rocks_put(RocksDB, "one", "àmímé níshíkíhéꜜbì"),
	    rocks_close(RocksDB)).
test(open_twice, [error(rocks_error(_),_), % TODO: error(rocks_error('IO error: lock hold by current process, acquire time 1657004085 acquiring thread 136471553664960: /tmp/test_rocksdb/LOCK: No locks available'),_)
		  cleanup(delete_db)]) :-
	test_db(Dir),
	setup_call_cleanup(
	    rocks_open(Dir, RocksDB1, []),
	    call_cleanup(rocks_open(Dir, RocksDB2, []),
			 rocks_close(RocksDB2)),
	    rocks_close(RocksDB1)).
test(batch, [Pairs == [zus-noot],
	     cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, RocksDB, []),
	rocks_put(RocksDB, aap, noot),
	rocks_get(RocksDB, aap, Value),
	rocks_batch(RocksDB,
		    [ delete(aap),
		      put(zus, Value)
		    ]),
	findall(K-V, rocks_enum(RocksDB, K, V), Pairs),
	rocks_close(RocksDB).

:- end_tests(rocks).

:- begin_tests(terms, [cleanup(delete_db)]).

test(basic, [Noot-Noot1 == noot(mies)-noot(1),
	     cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(term),
		     value(term)
		   ]),
	rocks_put(RocksDB, aap, noot(mies)),
	rocks_get(RocksDB, aap, Noot),
	rocks_put(RocksDB, aap(1), noot(1)),
	rocks_get(RocksDB, aap(1), Noot1),
	rocks_close(RocksDB).

:- end_tests(terms).

:- begin_tests(types, [cleanup(delete_db)]).

test(int32, [cleanup(delete_db)]) :-
	Min = -100, Max = 100,
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(atom),
		     value(int32)
		   ]),
	forall(between(Min, Max, I),
	       ( rocks_put(RocksDB, key, I),
		 assertion(rocks_get(RocksDB, key, I)))),
	rocks_close(RocksDB).

test(int64, [cleanup(delete_db)]) :-
	Min = -100, Max = 100,
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(atom),
		     value(int64)
		   ]),
	forall(between(Min, Max, I),
	       ( rocks_put(RocksDB, key, I),
		 assertion(rocks_get(RocksDB, key, I)))),
	rocks_close(RocksDB).

test(float, [cleanup(delete_db)]) :-
	Min = -100, Max = 100,
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(atom),
		     value(float)
		   ]),
	forall(between(Min, Max, I),
	       ( F is sin(I),
		 rocks_put(RocksDB, key, F),
		 rocks_get(RocksDB, key, F2),
		 assertion(abs(F-F2) < 0.00001))),
	rocks_close(RocksDB).

test(double, [cleanup(delete_db)]) :-
	Min = -100, Max = 100,
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(atom),
		     value(double)
		   ]),
	forall(between(Min, Max, I),
	       ( F is sin(I),
		 rocks_put(RocksDB, key, F),
		 rocks_get(RocksDB, key, F2),
		 assertion(F=:=F2))),
	rocks_close(RocksDB).

:- end_tests(types).

:- begin_tests(merge, [cleanup(delete_db)]).

test(set, [FinalOk == Final,
	   cleanup(delete_db)]) :-
	N = 100,
	numlist(1, N, FinalOk),
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ merge(merge),
		     value(term)
		   ]),
	rocks_put(DB, set, []),
	forall(between(1, N, I),
	       (   rocks_merge(DB, set, [I]),
		   (   I mod 10 =:= 0
		   ->  rocks_get(DB, set, Set),
		       assertion(numlist(1, I, Set))
		   ;   true
		   )
	       )),
	rocks_get(DB, set, Final),
	rocks_close(DB).
test(new, [Final == [1],
	   cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ merge(merge),
		     value(term)
		   ]),
	rocks_merge(DB, empty, [1]),
	rocks_get(DB, empty, Final),
	rocks_close(DB).

merge(partial, _Key, Left, Right, Result) :-
	debug(merge, 'Merge partial ~p ~p', [Left, Right]),
	ord_union(Left, Right, Result).
merge(full, _Key, Initial, Additions, Result) :-
	debug(merge, 'Merge full ~p ~p', [Initial, Additions]),
	append([Initial|Additions], List),
	sort(List, Result).

:- end_tests(merge).

:- begin_tests(builtin_merge, [cleanup(delete_db)]).

test(merge, [Final == FinalOk,
	     cleanup(delete_db)]) :-
	N = 100,
	numlist(1, N, FinalOk),
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ value(list(int64))
		   ]),
	forall(between(1, N, I),
	       (   rocks_merge(DB, set, I),
		   (   I mod 10 =:= 0
		   ->  rocks_get(DB, set, Set),
		       assertion(numlist(1, I, Set))
		   ;   true
		   )
	       )),
	rocks_get(DB, set, Final),
	rocks_close(DB).
test(put_set, [Final == [3,5,7],
	       cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ value(set(int64))
		   ]),
	rocks_put(DB, set, [5,3,7,5]),
	rocks_get(DB, set, Final),
	rocks_close(DB).
test(merge_set, [Final == [3,5],
		 cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ value(set(int64))
		   ]),
	rocks_put(DB, set, [3]),
	rocks_merge(DB, set, 5),
	rocks_merge(DB, set, 3),
	rocks_get(DB, set, Final),
	rocks_close(DB).

:- end_tests(builtin_merge).

:- begin_tests(properties, [cleanup(delete_db)]).

test(basic, [cleanup(delete_db)]) :-
	test_db(Dir),
	rocks_open(Dir, DB,
		   [ key(term),
		     value(term)
		   ]),
	rocks_put(DB, aap, noot(mies)),
	rocks_put(DB, aap(1), noot(1)),
	rocks_property(DB, estimate_num_keys(Num)),
	assertion(integer(Num)),
	rocks_close(DB).

:- end_tests(properties).

:- begin_tests(enum, [cleanup(delete_db)]).

test(enum,
     [ Keys == ["aap", "aapje", "noot"],
       cleanup(delete_db)
     ]) :-
	test_db(Dir),
	rocks_open(Dir, RocksDB,
		   [ key(string),
		     value(term)
		   ]),
	rocks_put(RocksDB, aap, 1),
	rocks_put(RocksDB, aapje, 2),
	rocks_put(RocksDB, noot, 3),
	findall(Key, rocks_enum_from(RocksDB, Key, _, aap), Keys),
	rocks_enum_prefix(RocksDB, Rest, _, aapj),
	assertion(Rest == "e"),
	rocks_close(RocksDB).

:- end_tests(enum).

:- begin_tests(alias, [cleanup(delete_db)]).

% rocks:basic, but with a named database
test(basic, [Noot == noot,
             setup(setup_db(Dir, DB, [alias('DB')])),
             cleanup(cleanup_db(Dir, DB))]) :-
	assertion(DB == 'DB'),
	rocks_put('DB', aap, noot),
	rocks_get('DB', aap, Noot),
	rocks_delete('DB', aap),
 	assertion(\+ rocks_get('DB', aap, _)).

:- end_tests(alias).

		 /*******************************
		 *	       UTIL		*
		 *******************************/

test_db('/tmp/test_rocksdb').

delete_db :-
    test_db(DB),
    delete_db(DB).

delete_db(DB) :-
    (   exists_directory(DB)
    ->  delete_directory_and_contents(DB)
    ;   true
    ).

setup_db(Dir, RocksDB) :-
    setup_db(Dir, RocksDB, []).

setup_db(Dir, RocksDB, Options) :-
    test_db(Dir),
    rocks_open(Dir, RocksDB, Options).

% cleanup_db/2 does its best to close the database because, if there
% is an error in the test case and the database isn't closed, it will
% cause spurious errors from all the subsequent tests.
cleanup_db(Dir, RocksDB) :-
    catch(ignore(rocks_close(RocksDB)), _, true),
    delete_db(Dir).
