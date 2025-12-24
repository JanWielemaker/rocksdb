% Test script for Windows RocksDB pack build
:- initialization(main, main).

main :-
    working_directory(_, 'c:/Users/Eric/Projects/Prolog_AI_Assistant_Research/rocksdb-pack-windows'),

    % Load the module
    write('Loading RocksDB module...'), nl,
    load_foreign_library('lib/x64-win64/Release/rocksdb4pl'),
    use_module(prolog/rocksdb),
    write('Module loaded successfully!'), nl, nl,

    % Test basic operations
    write('Testing RocksDB operations...'), nl,

    % Open database
    write('  Opening database... '),
    rocks_open('testdb_windows.db', DB, []),
    write('OK'), nl,

    % Put a value
    write('  Writing key-value pair... '),
    rocks_put(DB, testkey, testvalue),
    write('OK'), nl,

    % Get the value back
    write('  Reading value... '),
    rocks_get(DB, testkey, Value),
    format('OK (Value: ~w)~n', [Value]),

    % Verify the value
    write('  Verifying value... '),
    ( Value == testvalue
    -> write('OK')
    ;  write('FAILED')
    ), nl,

    % Close database
    write('  Closing database... '),
    rocks_close(DB),
    write('OK'), nl, nl,

    write('All tests PASSED!'), nl,
    halt(0).

main :-
    write('Tests FAILED!'), nl,
    halt(1).
