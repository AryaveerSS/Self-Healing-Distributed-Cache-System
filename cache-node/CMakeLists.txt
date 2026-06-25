cmake_minimum_required(VERSION 3.16)
project(cache_node VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -----------------------------------------------------------
# Build type defaults to Release for performance
# -----------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG   "-g -fsanitize=address,undefined")

# -----------------------------------------------------------
# Find dependencies
# -----------------------------------------------------------

# OpenSSL — required by cpp-httplib for HTTPS support
# (we use HTTP only, but httplib still needs it at link time)
find_package(OpenSSL REQUIRED)

# Threads
find_package(Threads REQUIRED)

# nlohmann/json — single header, placed in third_party/
# If using system install: find_package(nlohmann_json REQUIRED)
# We include it as header-only below.

# -----------------------------------------------------------
# Source files
# -----------------------------------------------------------
set(SOURCES
    src/lru_cache.cpp
    src/cache_server.cpp
    src/main.cpp
)

# -----------------------------------------------------------
# Executable
# -----------------------------------------------------------
add_executable(cache_node ${SOURCES})

# Include directories
target_include_directories(cache_node PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party   # httplib.h + nlohmann/json.hpp
)

# Link libraries
target_link_libraries(cache_node PRIVATE
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)

# -----------------------------------------------------------
# Compiler warnings
# -----------------------------------------------------------
target_compile_options(cache_node PRIVATE
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
)

# -----------------------------------------------------------
# Install
# -----------------------------------------------------------
install(TARGETS cache_node DESTINATION bin)

# -----------------------------------------------------------
# Testing (Phase 1 unit tests)
# -----------------------------------------------------------
option(BUILD_TESTS "Build unit tests" ON)

if(BUILD_TESTS)
    enable_testing()
    find_package(GTest QUIET)

    if(GTest_FOUND)
        add_executable(cache_tests
            tests/test_lru_cache.cpp
            src/lru_cache.cpp
        )
        target_include_directories(cache_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/third_party
        )
        target_link_libraries(cache_tests PRIVATE
            GTest::gtest_main
            Threads::Threads
        )
        include(GoogleTest)
        gtest_discover_tests(cache_tests)
    else()
        message(STATUS "GTest not found — skipping unit tests")
    endif()
endif()
