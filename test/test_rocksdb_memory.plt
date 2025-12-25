% test_rocksdb_memory.plt - Memory management tests for rocksdb
:- use_module(library(plunit)).
:- use_module(library(rocksdb)).
:- use_module(helpers/test_helpers_rocksdb).

:- begin_tests(memory_management, [cleanup(delete_db)]).

test(no_leak_open_close) :-
    test_db(Dir),
    statistics(cputime, T0),
    forall(between(1, 100, _),
           (setup_db(Dir, DB),
            cleanup_db(Dir, DB))),
    statistics(cputime, T1),
    Time is T1 - T0,
    format('100 open/close cycles: ~3f sec~n', [Time]),
    assertion(Time < 10.0).

test(large_values) :-
    test_db(Dir),
    setup_db(Dir, DB, [value(term)]),
    length(LargeList, 1000),
    maplist(=(test_value), LargeList),
    rocks_put(DB, large_key, LargeList),
    rocks_get(DB, large_key, Retrieved),
    length(Retrieved, 1000),
    cleanup_db(Dir, DB).

test(many_small_values) :-
    test_db(Dir),
    setup_db(Dir, DB),
    forall(between(1, 1000, N),
           (atom_concat(key_, N, Key),
            rocks_put(DB, Key, N))),
    rocks_get(DB, key_500, 500),
    cleanup_db(Dir, DB).

test(repeated_put_same_key) :-
    test_db(Dir),
    setup_db(Dir, DB),
    forall(between(1, 100, N),
           rocks_put(DB, same_key, N)),
    rocks_get(DB, same_key, 100),
    cleanup_db(Dir, DB).

:- end_tests(memory_management).

:- begin_tests(blob_cleanup, [cleanup(delete_db)]).

test(blob_cleanup_on_close) :-
    test_db(Dir),
    rocks_open(Dir, DB, [alias(test_db_alias)]),
    rocks_put(DB, key, value),
    rocks_close(DB),
    delete_db(Dir).

test(handle_reuse) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, key1, value1),
    rocks_close(DB1),
    rocks_open(Dir, DB2, []),
    rocks_get(DB2, key1, value1),
    rocks_put(DB2, key2, value2),
    rocks_close(DB2),
    delete_db(Dir).

:- end_tests(blob_cleanup).

:- begin_tests(stress_operations, [cleanup(delete_db)]).

test(many_operations) :-
    test_db(Dir),
    setup_db(Dir, DB),
    forall(between(1, 500, N),
           (atom_concat(key_, N, Key),
            atom_concat(val_, N, Val),
            rocks_put(DB, Key, Val))),
    findall(K-V,
            (between(1, 500, N),
             atom_concat(key_, N, K),
             rocks_get(DB, K, V)),
            Pairs),
    length(Pairs, 500),
    cleanup_db(Dir, DB).

test(alternating_put_delete) :-
    test_db(Dir),
    setup_db(Dir, DB),
    forall(between(1, 100, _),
           (rocks_put(DB, temp, value),
            rocks_delete(DB, temp))),
    assertion(\+ rocks_get(DB, temp, _)),
    cleanup_db(Dir, DB).

:- end_tests(stress_operations).
