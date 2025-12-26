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

# Link SWI-Prolog using swipl.cmake's helper function if available
if(COMMAND target_link_swipl)
    message(STATUS "Using target_link_swipl() for SWI-Prolog linking")
    target_link_swipl(rocksdb4pl)
elseif(TARGET swipl::libswipl)
    message(STATUS "Using swipl::libswipl target for SWI-Prolog linking")
    target_link_libraries(rocksdb4pl PRIVATE swipl::libswipl)
else()
    # Fallback: Manual configuration for Windows
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

set_target_properties(rocksdb4pl PROPERTIES
    PREFIX ""
    SUFFIX ".dll"
    LIBRARY_OUTPUT_DIRECTORY "${OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${OUTPUT_DIR}"
)

# =============================================================================
# Copy RocksDB DLL Dependencies (Windows)
# =============================================================================
if(WIN32 AND TARGET RocksDB::rocksdb-shared)
    add_custom_command(TARGET rocksdb4pl POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E echo "Copying RocksDB DLL to ${OUTPUT_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:RocksDB::rocksdb-shared>"
            "${OUTPUT_DIR}"
        COMMENT "Copying RocksDB DLL dependencies"
    )
endif()

# =============================================================================
# Installation (for pack manager)
# =============================================================================
install(TARGETS rocksdb4pl
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
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
