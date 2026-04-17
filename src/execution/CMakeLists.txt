add_library(db_execution STATIC kv_executor.cpp)

target_include_directories(db_execution PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(db_execution PUBLIC db_storage db_core)
