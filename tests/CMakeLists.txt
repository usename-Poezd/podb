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

add_executable(wal_writer_test wal_writer_test.cpp)
target_link_libraries(wal_writer_test PRIVATE GTest::gtest_main db_wal)
gtest_discover_tests(wal_writer_test TEST_PREFIX "wal_writer.")

add_executable(checkpoint_test checkpoint_test.cpp)
target_link_libraries(checkpoint_test PRIVATE
    GTest::gtest_main
    db_checkpoint
    db_storage
    db_wal
)
gtest_discover_tests(checkpoint_test TEST_PREFIX "checkpoint.")

add_executable(kv_executor_wal_test kv_executor_wal_test.cpp)
target_link_libraries(kv_executor_wal_test PRIVATE
    GTest::gtest_main
    db_execution
    db_wal
    db_storage
)
gtest_discover_tests(kv_executor_wal_test TEST_PREFIX "kv_executor_wal.")

add_executable(tx_coordinator_wal_test tx_coordinator_wal_test.cpp)
target_link_libraries(tx_coordinator_wal_test PRIVATE
    GTest::gtest_main
    db_transaction
    db_router
    db_wal
    db_core
)
gtest_discover_tests(tx_coordinator_wal_test TEST_PREFIX "tx_coordinator_wal.")

add_executable(recovery_test recovery_test.cpp)
target_link_libraries(recovery_test PRIVATE
    GTest::gtest_main
    db_recovery
    db_checkpoint
    db_wal
    db_storage
)
gtest_discover_tests(recovery_test TEST_PREFIX "recovery.")
