add_library(db_async STATIC request_tracker.cpp)

target_include_directories(db_async PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(db_async PUBLIC db_core)
