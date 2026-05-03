add_library(db_transaction STATIC tx_coordinator.cpp)

target_link_libraries(db_transaction PUBLIC
    db_project_headers
    db_core
    db_wal
)

target_include_directories(db_transaction PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
