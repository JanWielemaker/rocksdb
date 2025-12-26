# SWI-Prolog RocksDB Pack - Windows Installation Guide

This guide explains how to build and use the RocksDB pack natively on Windows using MSVC and vcpkg.

## Prerequisites

### Required Software

1. **Windows 11** (or Windows 10 with recent updates)

2. **Visual Studio 2022** (Community, Professional, or BuildTools)
   - Download: https://visualstudio.microsoft.com/downloads/
   - Required workload: "Desktop development with C++"
   - Minimum components: MSVC v143, Windows SDK

3. **SWI-Prolog 10.0.0 or newer**
   - Download: https://www.swi-prolog.org/Download.html
   - Use the 64-bit Windows installer
   - Default installation path: `C:\Program Files\swipl`

4. **vcpkg** (Microsoft C/C++ Package Manager)
   - Installation instructions below

5. **Git for Windows**
   - Download: https://git-scm.com/download/win

6. **CMake** (if not in PATH)
   - Usually installed with Visual Studio
   - Or download: https://cmake.org/download/

---

## Quick Install via pack_install (Recommended)

If you have all prerequisites installed, you can install directly via SWI-Prolog's pack manager:

### Step 1: Pre-install RocksDB via vcpkg

```cmd
C:\vcpkg\vcpkg.exe install rocksdb[lz4,snappy,zlib,zstd]:x64-windows
```

This takes 30-40 minutes the first time (RocksDB is compiled from source).

### Step 2: Set Environment Variables

Open a command prompt and set:

```cmd
set VCPKG_ROOT=C:\vcpkg
set PATH=C:\Program Files\CMake\bin;%PATH%
```

Or make permanent via System Properties > Environment Variables.

### Step 3: Install via pack_install

```cmd
swipl -g "pack_install(rocksdb, [url('https://github.com/EricGT/rocksdb.git'), branch('feature/windows-vcpkg-support')]), halt"
```

### Step 4: Verify Installation

```prolog
?- use_module(library(rocksdb)).
true.

?- rocks_open('test.db', DB, []), rocks_put(DB, key, value), rocks_get(DB, key, V), rocks_close(DB).
V = value.
```

---

## Manual Installation Steps

### Step 1: Install vcpkg

If you don't already have vcpkg installed:

```cmd
# Clone vcpkg
cd C:\
git clone https://github.com/microsoft/vcpkg

# Bootstrap vcpkg
C:\vcpkg\bootstrap-vcpkg.bat
```

**Verify installation:**
```cmd
C:\vcpkg\vcpkg.exe version
```

---

### Step 2: Clone the RocksDB Pack

```cmd
# Navigate to your projects directory
cd C:\Users\YOUR_USERNAME\Projects

# Clone this repository
git clone https://github.com/EricGT/rocksdb.git
cd rocksdb
```

---

### Step 3: Install Dependencies via vcpkg

The `vcpkg.json` manifest file specifies all required dependencies.

```cmd
# Install RocksDB and its dependencies (first time takes ~30-40 minutes)
C:\vcpkg\vcpkg.exe install
```

**What gets installed:**
- RocksDB 10.4.2+ with MSVC
- lz4 compression library
- snappy compression library
- zlib compression library
- zstd compression library

**Note**: The first build takes significant time as RocksDB is compiled from source. Subsequent builds will use vcpkg's binary cache and complete in seconds.

---

### Step 4: Configure with CMake

```cmd
# Create build directory
mkdir build
cd build

# Configure (uses vcpkg toolchain)
cmake .. -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake
```

**Optional**: If SWI-Prolog is installed in a non-standard location:
```cmd
cmake .. -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake -DSWIPL_ROOT="C:/path/to/swipl"
```

---

### Step 5: Build

```cmd
# Build the pack (creates rocksdb4pl.dll)
cmake --build . --config Release
```

**Expected output:**
```
Building rocksdb4pl.vcxproj...
rocksdb4pl.vcxproj -> C:\...\rocksdb\build\Release\rocksdb4pl.dll
Copying RocksDB DLL to module directory...
```

The compiled module will be placed in `lib/x64-win64/rocksdb4pl.dll`.

---

### Step 6: Install Pack in SWI-Prolog

```cmd
# Return to pack root directory
cd ..

# Install pack from local directory
swipl -g "pack_install('.',[upgrade(true)]),halt"
```

---

### Step 7: Test the Installation

#### Quick Test (Interactive)

```cmd
swipl
```

```prolog
?- use_module(library(rocksdb)).
true.

?- rocks_open('test.db', DB, []).
DB = <rocksdb>(0xXXXXXXXX).

?- rocks_put(DB, mykey, myvalue).
true.

?- rocks_get(DB, mykey, Value).
Value = myvalue.

?- rocks_close(DB).
true.
```

#### Run Test Suite

```cmd
swipl test/test_rocksdb.pl
```

---

## Troubleshooting

### Issue: "SWI-Prolog not found"

**Error:**
```
CMake Error: SWI-Prolog not found at C:/Program Files/swipl
```

**Solution:**
Specify the correct path with `-DSWIPL_ROOT`:
```cmd
cmake .. -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake -DSWIPL_ROOT="C:/your/path/to/swipl"
```

---

### Issue: "vcpkg.exe is not recognized"

**Error:**
```
'vcpkg.exe' is not recognized as an internal or external command
```

**Solution:**
Use the full path to vcpkg:
```cmd
C:\vcpkg\vcpkg.exe install
```

Or add `C:\vcpkg` to your PATH environment variable.

---

### Issue: "Waiting to take filesystem lock on C:\vcpkg\.vcpkg-root"

**Error:**
```
waiting to take filesystem lock on C:\vcpkg\.vcpkg-root...
```

**Solution:**
Another vcpkg process is running. Either:
- Wait for it to complete, or
- Close other terminals/processes using vcpkg

---

### Issue: "rocksdb4pl.dll failed to load"

**Error:**
```
ERROR: library `rocksdb4pl' does not exist (No error)
```

**Solution:**
1. Verify the DLL was built: Check `lib/x64-win64/rocksdb4pl.dll` exists
2. Ensure RocksDB DLL is in the same directory
3. Check dependencies with:
   ```cmd
   dumpbin /dependents lib/x64-win64/rocksdb4pl.dll
   ```

---

### Issue: "libswipl.dll not found"

**Error (when running tests):**
```
The code execution cannot proceed because libswipl.dll was not found.
```

**Solution:**
Add SWI-Prolog's bin directory to PATH:
```cmd
set PATH=C:\Program Files\swipl\bin;%PATH%
```

Or run from the SWI-Prolog installation directory.

---

## Advanced Configuration

### Custom SWI-Prolog Location

```cmd
cmake .. -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ^
         -DSWIPL_ROOT="D:/Programs/swipl"
```

### Debug Build

```cmd
cmake --build . --config Debug
```

This creates `rocksdb4pl.dll` with debug symbols for troubleshooting.

### Clean Rebuild

```cmd
# Remove build directory
cd ..
rmdir /s /q build

# Rebuild from scratch
mkdir build
cd build
cmake .. -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake
cmake --build . --config Release
```

---

## Performance Notes

- **First build**: 30-40 minutes (RocksDB compilation)
- **Subsequent builds**: < 1 minute (vcpkg binary cache)
- **Incremental changes**: ~10 seconds (only rebuilds changed files)

---

## Uninstalling

### Remove Pack from SWI-Prolog

```prolog
?- pack_remove(rocksdb).
```

### Remove vcpkg Dependencies (Optional)

```cmd
# Remove project-local dependencies
rmdir /s /q vcpkg_installed
```

---

## Limitations

### Known Limitations on Windows

1. **Build time**: Initial RocksDB compilation takes 30+ minutes
2. **Disk space**: vcpkg build artifacts require ~2-3 GB
3. **MSVC only**: MinGW/Cygwin builds are not supported

### Differences from Unix Build

- Uses `CMakeLists.txt` instead of `Makefile`
- RocksDB via vcpkg instead of git submodule
- Requires Visual Studio (MSVC) compiler
- Module extension is `.dll` instead of `.so`

---

## Getting Help

### Reporting Issues

If you encounter problems:

1. Check this README's troubleshooting section
2. Verify prerequisites are installed correctly
3. Try a clean rebuild
4. Open an issue at: https://github.com/EricGT/rocksdb/issues

Include:
- Windows version
- Visual Studio version
- SWI-Prolog version (`swipl --version`)
- vcpkg version (`vcpkg version`)
- Complete error messages
- CMake configuration output

---

## Contributing

Contributions to improve Windows support are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Test thoroughly on Windows
4. Submit a pull request

---

## References

- [SWI-Prolog RocksDB Pack](https://www.swi-prolog.org/pack/list?p=rocksdb)
- [vcpkg Documentation](https://vcpkg.io/)
- [RocksDB on vcpkg](https://vcpkg.io/en/package/rocksdb)
- [CMake Documentation](https://cmake.org/documentation/)

---

## License

This pack follows the same license as the upstream SWI-Prolog RocksDB pack.
RocksDB itself is dual-licensed under GPL-2.0-only OR Apache-2.0.
