add_library(db_async INTERFACE)

target_include_directories(db_async INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(db_async INTERFACE db_core)
