add_library(db_router STATIC router.cpp)

target_link_libraries(db_router PUBLIC
    db_project_headers
    db_core
    db_storage
)

target_include_directories(db_router PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
