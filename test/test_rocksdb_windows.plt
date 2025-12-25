% test_rocksdb_windows.plt - Windows-specific tests for rocksdb low-level API
:- use_module(library(plunit)).
:- use_module(library(rocksdb)).
:- use_module(helpers/test_helpers_rocksdb).

:- begin_tests(windows_paths, [cleanup(delete_db)]).

test(windows_path_forward_slash) :-
    rocks_open('c:/temp/test_forward', DB, []),
    rocks_put(DB, key, value),
    rocks_get(DB, key, value),
    rocks_close(DB),
    delete_db('c:/temp/test_forward').

test(windows_path_backslash) :-
    rocks_open('c:\\temp\\test_backslash', DB, []),
    rocks_put(DB, key, value),
    rocks_get(DB, key, value),
    rocks_close(DB),
    delete_db('c:/temp/test_backslash').

test(windows_path_spaces) :-
    rocks_open('c:/temp/test db with spaces', DB, []),
    rocks_put(DB, key, value),
    rocks_get(DB, key, value),
    rocks_close(DB),
    delete_db('c:/temp/test db with spaces').

test(windows_path_relative) :-
    rocks_open('temp_test_db', DB, []),
    rocks_put(DB, key, value),
    rocks_get(DB, key, value),
    rocks_close(DB),
    delete_db('temp_test_db').

:- end_tests(windows_paths).

:- begin_tests(dll_loading, []).

test(dll_loaded) :-
    current_foreign_library(_, rocksdb4pl).

test(compression_available) :-
    test_db(Dir),
    rocks_open(Dir, DB, []),
    rocks_put(DB, test, data),
    rocks_get(DB, test, data),
    rocks_close(DB).

:- end_tests(dll_loading).

:- begin_tests(file_locking, [cleanup(delete_db)]).

test(case_insensitive_paths) :-
    rocks_open('c:/temp/TestDB', DB1, []),
    rocks_put(DB1, key, value1),
    rocks_close(DB1),
    rocks_open('c:/temp/testdb', DB2, []),
    rocks_get(DB2, key, Value),
    rocks_close(DB2),
    assertion(Value == value1),
    delete_db('c:/temp/TestDB').

test(exclusive_lock, [error(rocks_error(_,_))]) :-
    test_db(Dir),
    rocks_open(Dir, DB1, []),
    rocks_open(Dir, _DB2, []).

:- end_tests(file_locking).
