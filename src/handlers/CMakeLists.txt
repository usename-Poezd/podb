add_library(db_handlers INTERFACE)

target_link_libraries(db_handlers INTERFACE
    db_project_headers
    db_router
    db_api
    asio-grpc::asio-grpc
)

target_include_directories(db_handlers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
