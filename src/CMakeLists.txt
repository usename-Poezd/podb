add_library(db_project_headers INTERFACE)
target_include_directories(db_project_headers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(api)
add_subdirectory(core)
add_subdirectory(storage)
add_subdirectory(execution)
add_subdirectory(async)
add_subdirectory(router)
add_subdirectory(handlers)
add_subdirectory(transaction)

add_executable(db_engine main.cpp)

target_link_libraries(db_engine PRIVATE
    db_project_headers
    db_handlers
    db_router
    db_async
    db_execution
    db_transaction
    db_core
    Boost::program_options
    Threads::Threads
)

