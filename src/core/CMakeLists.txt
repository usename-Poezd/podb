add_library(db_core STATIC worker.cpp)

target_link_libraries(db_core PUBLIC
    db_project_headers
    db_api
    asio-grpc::asio-grpc
    concurrentqueue::concurrentqueue
    PkgConfig::JEMALLOC
)

target_include_directories(db_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
