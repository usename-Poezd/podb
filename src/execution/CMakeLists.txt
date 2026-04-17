add_library(db_execution INTERFACE)

target_include_directories(db_execution INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(db_execution INTERFACE db_storage db_core)
