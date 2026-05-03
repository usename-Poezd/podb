add_library(db_wal STATIC wal_record.cpp wal_writer.cpp wal_reader.cpp)
target_link_libraries(db_wal PUBLIC db_core)
target_include_directories(db_wal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
