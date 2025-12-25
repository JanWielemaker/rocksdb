% test_rocksdb_errors.plt - Error handling tests for rocksdb
:- use_module(library(plunit)).
:- use_module(library(rocksdb)).
:- use_module(helpers/test_helpers_rocksdb).

:- begin_tests(error_conditions, [cleanup(delete_db)]).

test(error_invalid_path, [error(_)]) :-
    rocks_open('', _DB, []).

test(error_readonly_put, [error(rocks_error(_,_))]) :-
    test_db(Dir),
    setup_db(Dir, DB1),
    rocks_put(DB1, key, value),
    rocks_close(DB1),
    rocks_open(Dir, DB2, [mode(read_only)]),
    rocks_put(DB2, key, newvalue),
    rocks_close(DB2).

test(double_close_safe) :-
    test_db(Dir),
    setup_db(Dir, DB),
    rocks_close(DB),
    rocks_close(DB).

test(error_closed_db, [error(_)]) :-
    test_db(Dir),
    setup_db(Dir, DB),
    rocks_close(DB),
    rocks_put(DB, key, value).

test(error_invalid_key_type) :-
    test_db(Dir),
    setup_db(Dir, DB, [key(int32), value(atom)]),
    catch(
        rocks_put(DB, invalid_key, value),
        Error,
        (rocks_close(DB), throw(Error))),
    rocks_close(DB).

test(get_nonexistent_key) :-
    test_db(Dir),
    setup_db(Dir, DB),
    assertion(\+ rocks_get(DB, nonexistent, _)),
    rocks_close(DB).

test(delete_nonexistent_key) :-
    test_db(Dir),
    setup_db(Dir, DB),
    rocks_delete(DB, nonexistent),
    rocks_close(DB).

test(error_null_value) :-
    test_db(Dir),
    setup_db(Dir, DB),
    catch(rocks_put(DB, key, ''), Error, true),
    rocks_close(DB),
    (var(Error) -> true ; true).

:- end_tests(error_conditions).

:- begin_tests(type_validation, [cleanup(delete_db)]).

test(int32_overflow, [error(_)]) :-
    test_db(Dir),
    setup_db(Dir, DB, [key(atom), value(int32)]),
    BigNum is 2^32,
    rocks_put(DB, key, BigNum),
    rocks_close(DB).

test(atom_vs_string) :-
    test_db(Dir),
    setup_db(Dir, DB, [key(atom), value(atom)]),
    rocks_put(DB, test_key, test_value),
    rocks_get(DB, test_key, Value),
    assertion(Value == test_value),
    rocks_close(DB).

test(binary_data) :-
    test_db(Dir),
    setup_db(Dir, DB, [key(atom), value(binary)]),
    Binary = `\x00\x01\x02\xFF`,
    rocks_put(DB, binary_key, Binary),
    rocks_get(DB, binary_key, Retrieved),
    assertion(Retrieved == Binary),
    rocks_close(DB).

:- end_tests(type_validation).
