# Installing rocksdb Pack on Windows with vcpkg

## Quick Start (Manual Build - Recommended)

**Prerequisites**:
- Visual Studio 2022 Build Tools with MSVC and Windows SDK
- vcpkg installed at `C:\vcpkg` with RocksDB: `vcpkg install rocksdb[lz4,snappy,zlib,zstd]:x64-windows`
- SWI-Prolog 9.3.12 or later
- CMake and Git (standalone installations)

**Steps** (run in Developer Command Prompt for VS 2022):

```cmd
REM Clone the repository
git clone --branch feature/windows-vcpkg-support https://github.com/EricGT/rocksdb.git C:\rocksdb-pack
cd C:\rocksdb-pack

REM Build with CMake
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ..
cmake --build . --config Release
cmake --install .

REM Test
swipl -g "use_module(library(rocksdb)), writeln('rocksdb loaded successfully'), halt"
```

---

## Why pack_install Doesn't Work

**Problem**: SWI-Prolog's `pack_install` does not pass `-DCMAKE_TOOLCHAIN_FILE` to CMake, even when `VCPKG_ROOT` is set.

**Root Cause**: The pack build system calls CMake without the toolchain file argument:
```
cmake -DSWIPL=... -DCMAKE_BUILD_TYPE=Release -G Ninja ..
```

Without `-DCMAKE_TOOLCHAIN_FILE`, CMake cannot find vcpkg-installed packages like RocksDB.

**Why auto-detection doesn't work**: CMakeLists.txt can set `CMAKE_TOOLCHAIN_FILE` based on `VCPKG_ROOT`, but this happens *after* CMake has already started processing and is too late for some operations.

---

## Attempted Workarounds

### 1. Conditional Module Loading ❌

**File**: `pack.pl`
```prolog
:- if(current_prolog_flag(windows, true)).
:- use_module(cmake_options).
:- endif.
```

**Why it failed**: `pack.pl` is read as data by the pack installer, not executed as code. Conditional directives never run.

### 2. PowerShell Wrapper Script ❌

**File**: `install-pack-windows.ps1`

Sets up MSVC environment and VCPKG_ROOT before calling `pack_install`:
```powershell
$env:VCPKG_ROOT = "C:\vcpkg"
$env:VCPKG_DEFAULT_TRIPLET = "x64-windows"
& swipl -g "pack_install(rocksdb, [...])" -t halt
```

**Why it failed**: Environment variables are set correctly, but SWI-Prolog's pack build system still doesn't pass `-DCMAKE_TOOLCHAIN_FILE` to CMake.

### 3. Manual CMake Build ✅

**Works because**: You explicitly pass the toolchain file to CMake:
```cmd
cmake -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ..
```

---

## How the Manual Build Works

1. **CMakeLists.txt features**:
   - Sets `VCPKG_TARGET_TRIPLET=x64-windows` before `project()` to force x64 architecture
   - Includes vcpkg.json manifest for automatic dependency installation
   - Has fallback logic for finding SWI-Prolog (SWIPL_HOME_DIR, default paths)
   - Copies RocksDB DLL dependencies to output directory

2. **vcpkg.json manifest**:
   ```json
   {
     "name": "rocksdb4pl",
     "version": "0.14.4",
     "dependencies": [
       {
         "name": "rocksdb",
         "features": ["lz4", "snappy", "zlib", "zstd"]
       }
     ]
   }
   ```
   When CMake runs with the vcpkg toolchain, it automatically installs RocksDB if missing.

3. **Installation**: `cmake --install .` copies:
   - `rocksdb4pl.dll` → `%APPDATA%\swi-prolog\pack\rocksdb\lib\x64-win64\`
   - RocksDB DLL and dependencies

---

## Troubleshooting

### Error: "Could not find a configuration file for package RocksDB"

**Cause**: vcpkg toolchain not loaded.

**Fix**: Ensure you pass `-DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake` to CMake.

### Error: "Cannot find MinGW and/or MSYS"

**Cause**: Not running in MSVC environment.

**Fix**: Use "Developer Command Prompt for VS 2022" instead of regular cmd/PowerShell.

### DLL not found when loading library(rocksdb)

**Cause**: RocksDB DLL not in PATH or pack directory.

**Fix**: The build should auto-copy DLLs. Manually copy if needed:
```cmd
copy C:\vcpkg\installed\x64-windows\bin\rocksdb.dll %APPDATA%\swi-prolog\pack\rocksdb\lib\x64-win64\
```

### Wrong architecture (x86 vs x64)

**Cause**: vcpkg defaulted to x86-windows triplet.

**Fix**: CMakeLists.txt now forces x64-windows. If still issues, check:
```cmd
vcpkg list rocksdb
```
Should show `rocksdb:x64-windows`.

---

## For Pack Maintainers

To make `pack_install` work with vcpkg on Windows, the SWI-Prolog pack build system would need to:

1. Support injecting CMake arguments via `build_cmake:cmake_option/2` (currently ignored)
2. OR: Recognize `VCPKG_ROOT` and automatically pass `-DCMAKE_TOOLCHAIN_FILE`
3. OR: Support a `cmake_config.pl` that's executed (not just read as data) before build

Current limitation tracked in: https://github.com/EricGT/rocksdb/tree/feature/windows-vcpkg-support

---

## References

- [vcpkg Getting Started](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started)
- [vcpkg with CMake](https://learn.microsoft.com/en-us/vcpkg/users/buildsystems/cmake-integration)
- [SWI-Prolog Pack System](https://www.swi-prolog.org/pack/list)
- [Windows Sandbox Testing Plan](../../.claude/plans/valiant-sauteeing-clock.md)
