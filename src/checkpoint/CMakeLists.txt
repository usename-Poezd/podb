add_library(db_checkpoint STATIC checkpoint_writer.cpp checkpoint_reader.cpp)
target_link_libraries(db_checkpoint PUBLIC db_project_headers db_wal db_storage)
target_include_directories(db_checkpoint PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
