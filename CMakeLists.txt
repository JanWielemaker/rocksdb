cmake_minimum_required(VERSION 3.15)

# Auto-configure vcpkg toolchain on Windows if not already set
if(WIN32 AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if(DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
        message(STATUS "Using vcpkg toolchain from VCPKG_ROOT: ${CMAKE_TOOLCHAIN_FILE}")
    elseif(EXISTS "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE "C:/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
        message(STATUS "Using vcpkg toolchain from default location: ${CMAKE_TOOLCHAIN_FILE}")
    else()
        message(WARNING "vcpkg toolchain not found. RocksDB may not be found. Set VCPKG_ROOT environment variable or CMAKE_TOOLCHAIN_FILE.")
    endif()
endif()

project(swi-prolog-rocksdb VERSION 0.14.4)

# Platform detection
if(WIN32)
    set(BUILDING_ON_WINDOWS TRUE)
    message(STATUS "Building for Windows with MSVC")
endif()

# Compiler requirements
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find RocksDB package (via vcpkg on Windows)
find_package(RocksDB CONFIG REQUIRED)
message(STATUS "Found RocksDB: ${RocksDB_DIR}")

# SWI-Prolog integration
if(BUILDING_ON_WINDOWS)
    # Windows: Use configurable paths with sensible defaults
    set(SWIPL_ROOT "C:/Program Files/swipl" CACHE PATH "SWI-Prolog installation directory")

    if(NOT EXISTS "${SWIPL_ROOT}")
        message(FATAL_ERROR "SWI-Prolog not found at ${SWIPL_ROOT}. Set SWIPL_ROOT to your SWI-Prolog installation.")
    endif()

    set(SWIPL_INCLUDE_DIR "${SWIPL_ROOT}/include")
    set(SWIPL_LIBRARY "${SWIPL_ROOT}/bin/libswipl.lib")
    set(SWIPL_MODULE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib/x64-win64")
    set(MODULE_EXT "dll")

    message(STATUS "SWI-Prolog include: ${SWIPL_INCLUDE_DIR}")
    message(STATUS "SWI-Prolog library: ${SWIPL_LIBRARY}")
    message(STATUS "Module output directory: ${SWIPL_MODULE_DIR}")

    # Create output directory if it doesn't exist
    file(MAKE_DIRECTORY "${SWIPL_MODULE_DIR}")
else()
    # Unix: Query swipl for paths
    find_program(SWIPL_EXECUTABLE swipl REQUIRED)

    execute_process(
        COMMAND ${SWIPL_EXECUTABLE} --dump-runtime-variables
        OUTPUT_VARIABLE SWIPL_VARS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE SWIPL_RESULT
    )

    if(NOT SWIPL_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to query SWI-Prolog runtime variables")
    endif()

    # Parse variables (simplified - may need enhancement)
    string(REGEX MATCH "PLBASE=\"([^\"]+)\"" _ "${SWIPL_VARS}")
    set(SWIPL_BASE "${CMAKE_MATCH_1}")

    string(REGEX MATCH "PLARCH=\"([^\"]+)\"" _ "${SWIPL_VARS}")
    set(SWIPL_ARCH "${CMAKE_MATCH_1}")

    set(SWIPL_INCLUDE_DIR "${SWIPL_BASE}/include")
    set(SWIPL_LIBRARY "${SWIPL_BASE}/lib/${SWIPL_ARCH}/libswipl.so")
    set(SWIPL_MODULE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib/${SWIPL_ARCH}")
    set(MODULE_EXT "so")

    file(MAKE_DIRECTORY "${SWIPL_MODULE_DIR}")
endif()

# Foreign library target - MODULE type creates a shared library that can be loaded dynamically
add_library(rocksdb4pl MODULE cpp/rocksdb4pl.cpp)

# Include directories
target_include_directories(rocksdb4pl PRIVATE
    ${SWIPL_INCLUDE_DIR}
)

# Link libraries
target_link_libraries(rocksdb4pl PRIVATE
    RocksDB::rocksdb
    ${SWIPL_LIBRARY}
)

# Set output properties
set_target_properties(rocksdb4pl PROPERTIES
    PREFIX ""  # No lib prefix on the output file
    SUFFIX ".${MODULE_EXT}"
    LIBRARY_OUTPUT_DIRECTORY "${SWIPL_MODULE_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${SWIPL_MODULE_DIR}"  # For DLLs on Windows
)

# Windows-specific: Copy RocksDB DLL dependencies to module directory
if(BUILDING_ON_WINDOWS)
    # Note: RocksDB::rocksdb-shared is the shared library target
    # We need to copy the DLL to the module directory so SWI-Prolog can find it
    add_custom_command(TARGET rocksdb4pl POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E echo "Copying RocksDB DLL to module directory..."
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "$<TARGET_FILE:RocksDB::rocksdb-shared>"
            "${SWIPL_MODULE_DIR}"
        COMMENT "Copying RocksDB DLL dependencies"
    )

    # Also copy other potential DLL dependencies
    # vcpkg usually handles this via vcpkg_copy_dlls but we're doing it manually
    message(STATUS "DLL dependencies will be copied to ${SWIPL_MODULE_DIR}")
endif()

# Installation (for pack manager)
install(TARGETS rocksdb4pl
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
)

# Display build summary
message(STATUS "========================================")
message(STATUS "SWI-Prolog RocksDB Pack Configuration")
message(STATUS "========================================")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ Standard: C++${CMAKE_CXX_STANDARD}")
message(STATUS "RocksDB target: RocksDB::rocksdb")
message(STATUS "Output module: ${SWIPL_MODULE_DIR}/rocksdb4pl.${MODULE_EXT}")
message(STATUS "========================================")
