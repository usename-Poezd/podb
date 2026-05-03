add_executable(smoke_test smoke_test.cpp)
target_link_libraries(smoke_test PRIVATE GTest::gtest_main)
gtest_discover_tests(smoke_test)

add_executable(tx_coordinator_test tx_coordinator_test.cpp)
target_link_libraries(tx_coordinator_test PRIVATE
    GTest::gtest_main
    db_transaction
    db_router
    db_core
)
gtest_discover_tests(tx_coordinator_test TEST_PREFIX tx_coordinator.)

add_executable(storage_mvcc_test storage_mvcc_test.cpp)
target_link_libraries(storage_mvcc_test PRIVATE
    GTest::gtest_main
    db_storage
)
gtest_discover_tests(storage_mvcc_test TEST_PREFIX "storage_mvcc.")

add_executable(kv_executor_test kv_executor_test.cpp)
target_link_libraries(kv_executor_test PRIVATE
    GTest::gtest_main
    db_execution
    db_storage
)
gtest_discover_tests(kv_executor_test TEST_PREFIX "kv_executor.")

add_executable(wal_record_test wal_record_test.cpp)
target_link_libraries(wal_record_test PRIVATE GTest::gtest_main db_wal)
gtest_discover_tests(wal_record_test TEST_PREFIX "wal_record.")
