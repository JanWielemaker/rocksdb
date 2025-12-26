name(rocksdb).
title('SWI-Prolog interface to RocksDB').
version('0.14.4').
requires(prolog:c_cxx(_)). % requires(prolog >= "9.3.12").
pack_version(2).
keywords([database, embedded, nosql, 'RocksDB']).
author('Jan Wielemaker', 'jan@swi-prolog.org').
packager( 'Jan Wielemaker', 'jan@swi-prolog.org' ).
maintainer( 'Jan Wielemaker', 'jan@swi-prolog.org' ).
home( 'https://github.com/JanWielemaker/rocksdb' ).
download('https://github.com/EricGT/rocksdb.git').

% Load vcpkg cmake options on Windows for pack_install integration
:- if(current_prolog_flag(windows, true)).
:- use_module(cmake_options).
:- endif.
