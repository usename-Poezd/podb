cmake_minimum_required(VERSION 3.20)
set(VCPKG_INSTALL_OPTIONS "--no-print-usage" CACHE STRING "" FORCE)
project(db_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pthread")

# Ищем все системные пакеты глобально
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS program_options)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(asio-grpc CONFIG REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(JEMALLOC REQUIRED IMPORTED_TARGET jemalloc)

add_subdirectory(src)

enable_testing()
find_package(GTest CONFIG REQUIRED)
include(GoogleTest)
add_subdirectory(tests)
