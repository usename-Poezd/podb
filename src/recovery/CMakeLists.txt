add_library(db_recovery STATIC recovery_manager.cpp)
target_link_libraries(db_recovery PUBLIC db_project_headers db_wal db_checkpoint db_storage)
target_include_directories(db_recovery PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
