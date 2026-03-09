add_subdirectory(api)
add_subdirectory(core)
add_subdirectory(storage)
add_subdirectory(router)
add_subdirectory(handlers)

add_executable(db_engine main.cpp)

target_link_libraries(db_engine PRIVATE
    db_handlers
    db_router
    db_core
    Boost::program_options
    Threads::Threads
)
