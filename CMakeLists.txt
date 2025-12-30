cmake_minimum_required(VERSION 3.15)

# =============================================================================
# vcpkg Toolchain Configuration
# =============================================================================
# For standalone builds, auto-detect vcpkg toolchain.
# For pack_install, cmake_options.pl injects this via cmake_option/2.
if(WIN32 AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if(DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
        message(STATUS "Using vcpkg toolchain from VCPKG_ROOT: ${CMAKE_TOOLCHAIN_FILE}")
    elseif(EXISTS "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE "C:/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
        message(STATUS "Using vcpkg toolchain from default location: ${CMAKE_TOOLCHAIN_FILE}")
    else()
        message(WARNING "vcpkg toolchain not found. Set VCPKG_ROOT or CMAKE_TOOLCHAIN_FILE.")
    endif()
endif()
# Force x64 architecture for vcpkg on Windows (must be set before project())
if(WIN32 AND NOT DEFINED VCPKG_TARGET_TRIPLET)
    set(VCPKG_TARGET_TRIPLET "x64-windows" CACHE STRING "vcpkg target triplet")
    message(STATUS "Using vcpkg triplet: ${VCPKG_TARGET_TRIPLET}")
endif()

project(rocksdb4pl VERSION 0.14.4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# =============================================================================
# SWI-Prolog Integration via swipl.cmake
# =============================================================================
# swipl.cmake provides:
#   - swipl::libswipl target
#   - target_link_swipl() function
#   - swipl_module_dir variable
#
# When invoked by pack_install, SWIPL_HOME_DIR environment variable is set.
# For standalone builds, fall back to default installation path.

set(SWIPL_CMAKE_LOADED FALSE)

if(DEFINED ENV{SWIPL_HOME_DIR})
    # pack_install context - use environment variable
    set(SWIPL_CMAKE_PATH "$ENV{SWIPL_HOME_DIR}/cmake/swipl.cmake")
    if(EXISTS "${SWIPL_CMAKE_PATH}")
        message(STATUS "Loading swipl.cmake from pack_install context: ${SWIPL_CMAKE_PATH}")
        include("${SWIPL_CMAKE_PATH}")
        set(SWIPL_CMAKE_LOADED TRUE)
    endif()
endif()

if(NOT SWIPL_CMAKE_LOADED AND WIN32)
    # Standalone Windows build - check default installation
    set(SWIPL_CMAKE_PATH "C:/Program Files/swipl/cmake/swipl.cmake")
    if(EXISTS "${SWIPL_CMAKE_PATH}")
        message(STATUS "Loading swipl.cmake from default location: ${SWIPL_CMAKE_PATH}")
        include("${SWIPL_CMAKE_PATH}")
        set(SWIPL_CMAKE_LOADED TRUE)
    endif()
endif()

if(NOT SWIPL_CMAKE_LOADED AND NOT WIN32)
    # Unix standalone - try to find swipl and load its cmake module
    find_program(SWIPL_EXECUTABLE swipl)
    if(SWIPL_EXECUTABLE)
        execute_process(
            COMMAND ${SWIPL_EXECUTABLE} --dump-runtime-variables
            OUTPUT_VARIABLE SWIPL_VARS
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE SWIPL_RESULT
        )
        if(SWIPL_RESULT EQUAL 0)
            string(REGEX MATCH "PLBASE=\"([^\"]+)\"" _ "${SWIPL_VARS}")
            set(SWIPL_BASE "${CMAKE_MATCH_1}")
            set(SWIPL_CMAKE_PATH "${SWIPL_BASE}/cmake/swipl.cmake")
            if(EXISTS "${SWIPL_CMAKE_PATH}")
                message(STATUS "Loading swipl.cmake from swipl query: ${SWIPL_CMAKE_PATH}")
                include("${SWIPL_CMAKE_PATH}")
                set(SWIPL_CMAKE_LOADED TRUE)
            endif()
        endif()
    endif()
endif()

if(NOT SWIPL_CMAKE_LOADED)
    message(WARNING "Could not load swipl.cmake - falling back to manual configuration")
endif()

# =============================================================================
# Find RocksDB via vcpkg
# =============================================================================
find_package(RocksDB CONFIG REQUIRED)
message(STATUS "Found RocksDB: ${RocksDB_DIR}")

# =============================================================================
# Build Foreign Module
# =============================================================================
add_library(rocksdb4pl MODULE cpp/rocksdb4pl.cpp)

# Link RocksDB
target_link_libraries(rocksdb4pl PRIVATE RocksDB::rocksdb)

# =============================================================================
# Create swipl::libswipl Target if Missing
# =============================================================================
# swipl.cmake has a bug on Windows where it looks for SWIPLTargets.cmake
# in the wrong directory (parent of swipl_home_dir instead of lib/cmake/swipl).
# Work around by creating the target manually if it doesn't exist.
if(NOT TARGET swipl::libswipl)
    if(DEFINED ENV{SWIPL_HOME_DIR})
        set(SWIPL_HOME "$ENV{SWIPL_HOME_DIR}")
    elseif(EXISTS "C:/Program Files/swipl")
        set(SWIPL_HOME "C:/Program Files/swipl")
    endif()

    if(SWIPL_HOME AND EXISTS "${SWIPL_HOME}/include/SWI-Prolog.h")
        message(STATUS "Creating swipl::libswipl target manually (swipl.cmake workaround)")
        add_library(swipl::libswipl SHARED IMPORTED)
        set_target_properties(swipl::libswipl PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${SWIPL_HOME}/include"
            IMPORTED_LOCATION "${SWIPL_HOME}/bin/libswipl.dll"
            IMPORTED_IMPLIB "${SWIPL_HOME}/bin/libswipl.lib"
        )
    endif()
endif()

# Link SWI-Prolog using swipl.cmake's helper function if available
if(COMMAND target_link_swipl AND TARGET swipl::libswipl)
    message(STATUS "Using target_link_swipl() for SWI-Prolog linking")
    target_link_swipl(rocksdb4pl)
elseif(TARGET swipl::libswipl)
    message(STATUS "Using swipl::libswipl target for SWI-Prolog linking")
    target_link_libraries(rocksdb4pl PRIVATE swipl::libswipl)
else()
    # Final fallback: Manual configuration
    message(STATUS "Using manual SWI-Prolog configuration")
    set(SWIPL_ROOT "C:/Program Files/swipl" CACHE PATH "SWI-Prolog installation")
    if(EXISTS "${SWIPL_ROOT}/include/SWI-Prolog.h")
        target_include_directories(rocksdb4pl PRIVATE "${SWIPL_ROOT}/include")
        target_link_libraries(rocksdb4pl PRIVATE "${SWIPL_ROOT}/bin/libswipl.lib")
    else()
        message(FATAL_ERROR "SWI-Prolog not found. Set SWIPL_ROOT to your installation.")
    endif()
endif()

# =============================================================================
# Output Directory Configuration
# =============================================================================
# Use swipl_module_dir if set by swipl.cmake, otherwise use default
if(DEFINED swipl_module_dir AND NOT "${swipl_module_dir}" STREQUAL "")
    set(OUTPUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/${swipl_module_dir}")
    message(STATUS "Using module directory from swipl.cmake: ${OUTPUT_DIR}")
else()
    # Default for Windows standalone builds
    set(OUTPUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib/x64-win64")
    message(STATUS "Using default module directory: ${OUTPUT_DIR}")
endif()

file(MAKE_DIRECTORY "${OUTPUT_DIR}")

# Set output properties - explicitly set for each config to avoid subdirectories
set_target_properties(rocksdb4pl PROPERTIES
    PREFIX ""
    SUFFIX ".dll"
    LIBRARY_OUTPUT_DIRECTORY "${OUTPUT_DIR}"
    LIBRARY_OUTPUT_DIRECTORY_DEBUG "${OUTPUT_DIR}"
    LIBRARY_OUTPUT_DIRECTORY_RELEASE "${OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG "${OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY_RELEASE "${OUTPUT_DIR}"
)

# =============================================================================
# Copy RocksDB DLL Dependencies (Windows)
# =============================================================================
if(WIN32)
    # Find vcpkg bin directory for dependency DLLs
    if(DEFINED ENV{VCPKG_ROOT})
        set(VCPKG_BIN_DIR "$ENV{VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/bin")
    elseif(EXISTS "C:/vcpkg/installed/x64-windows/bin")
        set(VCPKG_BIN_DIR "C:/vcpkg/installed/x64-windows/bin")
    endif()

    if(VCPKG_BIN_DIR AND EXISTS "${VCPKG_BIN_DIR}")
        message(STATUS "vcpkg bin directory: ${VCPKG_BIN_DIR}")

        # Copy all vcpkg DLLs to pack's lib directory
        # This is sufficient - SWI-Prolog finds DLLs in the same directory as the module
        add_custom_command(TARGET rocksdb4pl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E echo "Copying vcpkg DLLs to ${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${VCPKG_BIN_DIR}/rocksdb-shared.dll" "${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${VCPKG_BIN_DIR}/lz4.dll" "${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${VCPKG_BIN_DIR}/snappy.dll" "${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${VCPKG_BIN_DIR}/zlib1.dll" "${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${VCPKG_BIN_DIR}/zstd.dll" "${OUTPUT_DIR}"
            COMMENT "Copying RocksDB DLL dependencies to pack lib directory"
        )
    elseif(TARGET RocksDB::rocksdb-shared)
        # Fallback: just copy the RocksDB DLL if vcpkg bin not found
        add_custom_command(TARGET rocksdb4pl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E echo "Copying RocksDB DLL to ${OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "$<TARGET_FILE:RocksDB::rocksdb-shared>"
                "${OUTPUT_DIR}"
            COMMENT "Copying RocksDB DLL dependencies"
        )
    endif()
endif()

# =============================================================================
# Installation (for pack manager)
# =============================================================================
# Set install prefix to the pack directory for pack_install
# This avoids installing to system directories like C:/Program Files
set(CMAKE_INSTALL_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}" CACHE PATH "" FORCE)

install(TARGETS rocksdb4pl
    LIBRARY DESTINATION "${swipl_module_dir}"
    RUNTIME DESTINATION "${swipl_module_dir}"
)

# =============================================================================
# Build Summary
# =============================================================================
message(STATUS "========================================")
message(STATUS "SWI-Prolog RocksDB Pack Configuration")
message(STATUS "========================================")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ Standard: C++${CMAKE_CXX_STANDARD}")
message(STATUS "swipl.cmake loaded: ${SWIPL_CMAKE_LOADED}")
message(STATUS "Output directory: ${OUTPUT_DIR}")
message(STATUS "========================================")
