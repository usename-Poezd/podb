add_library(db_storage STATIC storage_engine.cpp)
target_link_libraries(db_storage PUBLIC db_project_headers)
target_include_directories(db_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
