/*  Part of SWI-Prolog RocksDB Pack - Windows vcpkg Integration

    This module extends build_cmake:cmake_option/2 to inject the vcpkg
    toolchain file when building on Windows. This enables pack_install
    to automatically configure cmake with the correct toolchain.

    Usage:
        This module is automatically loaded by pack.pl on Windows.
        Users must have vcpkg installed at C:/vcpkg or set VCPKG_ROOT.

    Prerequisites:
        1. vcpkg installed with RocksDB:
           vcpkg install rocksdb[lz4,snappy,zlib,zstd]:x64-windows
        2. cmake on PATH
*/

:- module(rocksdb_cmake_options, []).

:- use_module(library(build/cmake), []).

:- multifile build_cmake:cmake_option/2.

%!  build_cmake:cmake_option(+State, -Option) is nondet.
%
%   Inject vcpkg toolchain file into cmake configuration on Windows.
%   This clause is picked up by pack_install's cmake build step.

build_cmake:cmake_option(_, Opt) :-
    current_prolog_flag(windows, true),
    vcpkg_toolchain_file(ToolchainFile),
    exists_file(ToolchainFile),
    format(atom(Opt), '-DCMAKE_TOOLCHAIN_FILE=~w', [ToolchainFile]).

%!  vcpkg_toolchain_file(-Path) is det.
%
%   Determine the path to vcpkg's CMake toolchain file.
%   Checks VCPKG_ROOT environment variable first, falls back to C:/vcpkg.

vcpkg_toolchain_file(ToolchainFile) :-
    getenv('VCPKG_ROOT', Root),
    !,
    atom_concat(Root, '/scripts/buildsystems/vcpkg.cmake', ToolchainFile).
vcpkg_toolchain_file('C:/vcpkg/scripts/buildsystems/vcpkg.cmake').
