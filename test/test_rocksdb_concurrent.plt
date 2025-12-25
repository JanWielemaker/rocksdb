% test_rocksdb_concurrent.plt - Concurrent access tests for rocksdb
:- use_module(library(plunit)).
:- use_module(library(rocksdb)).
:- use_module(helpers/test_helpers_rocksdb).

:- begin_tests(concurrent_access, [cleanup(delete_db)]).

test(concurrent_open_fails, [error(rocks_error(_,_))]) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_open(Dir, _DB2, []),
    rocks_close(DB1).

test(sequential_open_close) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, key, value1),
    rocks_close(DB1),
    rocks_open(Dir, DB2, []),
    rocks_get(DB2, key, value1),
    rocks_put(DB2, key, value2),
    rocks_close(DB2),
    rocks_open(Dir, DB3, []),
    rocks_get(DB3, key, value2),
    rocks_close(DB3).

test(readonly_after_write) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, key, value),
    rocks_close(DB1),
    rocks_open(Dir, DB2, [mode(read_only)]),
    rocks_get(DB2, key, value),
    rocks_close(DB2).

test(multiple_readonly_fails, [error(rocks_error(_,_))]) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_close(DB1),
    rocks_open(Dir, DB2, [mode(read_only)]),
    rocks_open(Dir, _DB3, [mode(read_only)]),
    rocks_close(DB2).

:- end_tests(concurrent_access).

:- begin_tests(persistence, [cleanup(delete_db)]).

test(data_persists_after_close) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, persistent_key, persistent_value),
    rocks_close(DB1),
    rocks_open(Dir, DB2, []),
    rocks_get(DB2, persistent_key, persistent_value),
    rocks_close(DB2).

test(deletion_persists) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, temp_key, temp_value),
    rocks_delete(DB1, temp_key),
    rocks_close(DB1),
    rocks_open(Dir, DB2, []),
    assertion(\+ rocks_get(DB2, temp_key, _)),
    rocks_close(DB2).

:- end_tests(persistence).
