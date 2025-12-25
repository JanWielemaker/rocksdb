% test_helpers_rocksdb.pl - Test helpers for rocksdb low-level testing
:- module(test_helpers_rocksdb, [
    test_db/1,            % Test database path
    delete_db/0,          % Delete test database
    delete_db/1,          % Delete specific database
    setup_db/2,           % Setup database with default options
    setup_db/3,           % Setup database with custom options
    cleanup_db/2          % Cleanup database (robust)
]).

:- use_module(library(rocksdb)).
:- use_module(library(filesex)).

%! test_db(-Path) is det.
%
% Provides the default test database path for Windows.

test_db('c:/temp/test_rocksdb').

%! delete_db is det.
%
% Delete the default test database.

delete_db :-
    test_db(DB),
    delete_db(DB).

%! delete_db(+DB) is det.
%
% Delete a specific database directory and all its contents.

delete_db(DB) :-
    (exists_directory(DB) ->
        delete_directory_and_contents(DB) ; true).

%! setup_db(-Dir, -RocksDB) is det.
%
% Setup a test database with default options.
% Deletes any existing database first.

setup_db(Dir, RocksDB) :-
    setup_db(Dir, RocksDB, []).

%! setup_db(-Dir, -RocksDB, +Options) is det.
%
% Setup a test database with custom options.
% Deletes any existing database first.

setup_db(Dir, RocksDB, Options) :-
    test_db(Dir),
    delete_db(Dir),
    rocks_open(Dir, RocksDB, Options).

%! cleanup_db(+Dir, +RocksDB) is det.
%
% Cleanup database - robust version that handles errors gracefully.
% Closes the database and deletes all files.

cleanup_db(Dir, RocksDB) :-
    catch((ignore(rocks_close(RocksDB)),
           delete_db(Dir)),
          _, true).
