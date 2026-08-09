add_library(db_bench_core STATIC
    options.cpp
    stats.cpp
    workload.cpp
    worker.cpp
)

target_link_libraries(db_bench_core PUBLIC
    db_api
    Boost::program_options
    Threads::Threads
)

add_executable(db_bench main.cpp)
target_link_libraries(db_bench PRIVATE db_bench_core)

add_executable(bench_stats_test bench_stats_test.cpp)
target_link_libraries(bench_stats_test PRIVATE db_bench_core GTest::gtest_main)
gtest_discover_tests(bench_stats_test TEST_PREFIX "bench.")
