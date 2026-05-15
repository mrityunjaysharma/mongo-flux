cmake_minimum_required(VERSION 3.16)
project(mg_clickhouse VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(BUILD_TESTS "Build test suite" OFF)
option(BUILD_SHARED "Build shared library" ON)

# Find dependencies
find_package(PkgConfig REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# MongoDB C++ Driver
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)

# We use ClickHouse HTTP interface (via libcurl), no clickhouse-cpp needed.

# nlohmann/json (header-only)
include(FetchContent)
FetchContent_Declare(json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(json)

# cpp-httplib (header-only)
FetchContent_Declare(httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.15.3
)
FetchContent_MakeAvailable(httplib)

# yaml-cpp for config
FetchContent_Declare(yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG 0.8.0
)
FetchContent_MakeAvailable(yaml-cpp)

# Source files
set(MG_CLICKHOUSE_SOURCES
    src/main.cpp
    src/config.cpp
    src/schema_mapping.cpp
    src/oplog_sync.cpp
    src/change_stream_sync.cpp
    src/expr_tree.cpp
    src/query_translator.cpp
    src/clickhouse_client.cpp
    src/mongo_proxy.cpp
    src/management_api.cpp
    src/routing.cpp
)

# Library target
add_library(mg_clickhouse_lib ${MG_CLICKHOUSE_SOURCES})
target_include_directories(mg_clickhouse_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(mg_clickhouse_lib PUBLIC
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    CURL::libcurl
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    nlohmann_json::nlohmann_json
    httplib::httplib
    yaml-cpp::yaml-cpp
)

# Executable
add_executable(mg_clickhouse src/main.cpp)
target_link_libraries(mg_clickhouse PRIVATE mg_clickhouse_lib)

# Install
install(TARGETS mg_clickhouse RUNTIME DESTINATION bin)
install(FILES config/mg-clickhouse.yaml DESTINATION etc/mg-clickhouse)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()
