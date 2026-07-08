# CMake integration for the hf_xet C/C++ API.
#
# Consumers add this directory and link the imported `xet_capi` target:
#
#   add_subdirectory(/path/to/xet-core/xet_capi xet_capi)
#   target_link_libraries(my_target PRIVATE xet_capi)
#
# The target carries the include directory for `hf_xet.h` (C API) and
# `xet_session.hpp` (header-only C++ wrapper, requires C++17).
#
# Requires a Rust toolchain; the library is built with `cargo build -p xet_capi`.
# Linking is static by default (set XET_CAPI_SHARED=ON for the shared library).
# CARGO_TARGET_DIR overrides are not supported; the workspace target directory
# is assumed to be <xet-core>/target.

cmake_minimum_required(VERSION 3.15)
project(xet_capi LANGUAGES C)

option(XET_CAPI_SHARED "Link the shared xet_capi library instead of the static one" OFF)

find_program(XET_CAPI_CARGO cargo REQUIRED)

set(XET_CAPI_CRATE_DIR  ${CMAKE_CURRENT_SOURCE_DIR})
set(XET_CAPI_TARGET_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../target)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(XET_CAPI_PROFILE     debug)
    set(XET_CAPI_CARGO_FLAGS "")
else()
    set(XET_CAPI_PROFILE     release)
    set(XET_CAPI_CARGO_FLAGS --release)
endif()

if(XET_CAPI_SHARED)
    set(XET_CAPI_LIB_FILE
        ${XET_CAPI_TARGET_DIR}/${XET_CAPI_PROFILE}/${CMAKE_SHARED_LIBRARY_PREFIX}xet_capi${CMAKE_SHARED_LIBRARY_SUFFIX})
else()
    set(XET_CAPI_LIB_FILE
        ${XET_CAPI_TARGET_DIR}/${XET_CAPI_PROFILE}/${CMAKE_STATIC_LIBRARY_PREFIX}xet_capi${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

# cargo tracks Rust source changes itself; run it on every build (no-op when fresh).
add_custom_target(xet_capi_cargo
    COMMAND ${XET_CAPI_CARGO} build -p xet_capi ${XET_CAPI_CARGO_FLAGS}
    WORKING_DIRECTORY ${XET_CAPI_CRATE_DIR}
    BYPRODUCTS ${XET_CAPI_LIB_FILE}
    VERBATIM USES_TERMINAL
    COMMENT "Building xet_capi with cargo (${XET_CAPI_PROFILE})"
)

if(XET_CAPI_SHARED)
    add_library(xet_capi SHARED IMPORTED GLOBAL)
else()
    add_library(xet_capi STATIC IMPORTED GLOBAL)
endif()
add_dependencies(xet_capi xet_capi_cargo)

set_target_properties(xet_capi PROPERTIES
    IMPORTED_LOCATION             ${XET_CAPI_LIB_FILE}
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# System libraries required by the Rust static library
# (from `cargo rustc -p xet_capi --release -- --print native-static-libs`).
if(NOT XET_CAPI_SHARED)
    if(APPLE)
        set_property(TARGET xet_capi APPEND PROPERTY INTERFACE_LINK_LIBRARIES
            "-framework Security" "-framework CoreFoundation" "-framework SystemConfiguration"
            "-framework IOKit" objc iconv)
    elseif(WIN32)
        set_property(TARGET xet_capi APPEND PROPERTY INTERFACE_LINK_LIBRARIES
            ws2_32 userenv bcrypt ntdll crypt32 secur32 ncrypt)
    elseif(UNIX)
        set_property(TARGET xet_capi APPEND PROPERTY INTERFACE_LINK_LIBRARIES
            pthread dl m)
    endif()
endif()
