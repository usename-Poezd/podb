#include <climits>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>

#include "execution/kv_executor.h"
#include "handlers/grpc_handler.h"
#include "router/router.h"
#include "storage/storage_engine.h"
#include "core/worker.h"
#include "core/core_dispatcher.h"
#include "transaction/tx_coordinator.h"

#include <filesystem>
#include "wal/wal_writer.h"
#include "recovery/recovery_manager.h"

namespace po = boost::program_options;
using namespace db;

int main(int argc, char *argv[]) {
  try {
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Produce help message")("cores,c", po::value<int>(),
                                                         "Number of worker threads/cores")(
        "port,p", po::value<int>()->default_value(9906), "TCP port for gRPC")(
        "data-dir,d", po::value<std::string>()->default_value("./data"), "Data directory for WAL/snapshots")(
        "repartition-on-recovery", po::bool_switch()->default_value(false),
        "Allow offline repartition when --cores differs from stored topology");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
      std::cout << desc << "\n";
      return 0;
    }

    int cores = 1;
    if (vm.count("cores")) {
      cores = vm["cores"].as<int>();
    } else {
      unsigned int hw = std::thread::hardware_concurrency();
      cores = (hw > 1) ? 2 : 1;
    }
    const int port = vm["port"].as<int>();
    const std::string data_dir = vm["data-dir"].as<std::string>();
    const bool allow_repartition = vm["repartition-on-recovery"].as<bool>();

    std::cout << "[Main] Starting STRICT Thread-per-Core engine.\n";
    std::cout << "[Main] Total cores: " << cores << ". Port: " << port << "\n";

    std::filesystem::create_directories(data_dir);

    auto stored_meta = RecoveryManager::ReadTopologyMeta(data_dir);
    uint64_t layout_epoch = stored_meta.has_value() ? stored_meta->layout_epoch : 1;
    bool needs_repartition = stored_meta.has_value() &&
        stored_meta->num_cores != static_cast<uint32_t>(cores);

    std::vector<std::unique_ptr<StorageEngine>> storages(cores);
    for (int i = 0; i < cores; ++i) {
      storages[i] = std::make_unique<StorageEngine>();
    }

    if (needs_repartition && !allow_repartition) {
      std::cerr << "[Main] Topology mismatch: stored num_cores="
                << stored_meta->num_cores << ", configured --cores=" << cores << "\n";
      std::cerr << "[Main] Use --repartition-on-recovery to allow offline repartition. Aborting.\n";
      return 1;
    }

    if (needs_repartition) {
      std::cout << "[Main] Repartition recovery: "
                << stored_meta->num_cores << " → " << cores << " cores\n";
      RecoveryManager::Repartition(data_dir, stored_meta->num_cores,
                                   static_cast<uint32_t>(cores), storages);
      layout_epoch = stored_meta->layout_epoch + 1;
    }

    if (!needs_repartition) {
      for (int i = 0; i < cores; ++i) {
        RecoveryManager::RecoverCore(i, data_dir, *storages[i]);
      }
    }

    std::vector<std::unique_ptr<WalWriter>> wal_writers(cores);
    for (int i = 0; i < cores; ++i) {
      wal_writers[i] = std::make_unique<WalWriter>(RecoveryManager::WalPath(data_dir, i));
    }

    std::vector<std::unique_ptr<Worker>> workers(cores);
    workers[0] = std::make_unique<Worker>(0, WorkerMode::Ingress, port);
    for (int i = 1; i < cores; ++i) {
      workers[i] = std::make_unique<Worker>(i, WorkerMode::WorkerOnly, port);
    }

    std::vector<Worker *> worker_ptrs;
    worker_ptrs.reserve(cores);
    for (auto &w : workers) {
      worker_ptrs.push_back(w.get());
    }

    std::vector<std::unique_ptr<KvExecutor>> executors;
    executors.reserve(cores);
    for (int i = 0; i < cores; ++i) {
      executors.push_back(std::make_unique<KvExecutor>(*storages[i], i, wal_writers[i].get()));
    }

    std::vector<std::unique_ptr<Router>> routers(cores);
    for (int i = 0; i < cores; ++i) {
      routers[i] = std::make_unique<Router>(i, worker_ptrs,
          [ex = executors[i].get(), wptr = worker_ptrs](Task task) mutable {
            Task resp = ex->Execute(std::move(task));
            int rtc = resp.reply_to_core;
            if (rtc >= 0 && rtc < static_cast<int>(wptr.size())) {
              wptr[rtc]->PushTask(std::move(resp));
            }
          });
    }

    // 1. Deferred dispatcher pointer — разрывает цикл GrpcHandler↔CoreDispatcher
    CoreDispatcher* dispatcher_ptr = nullptr;
    auto dispatch_fn = [&dispatcher_ptr](Task t) {
      dispatcher_ptr->Dispatch(std::move(t));
    };

    // 2. GrpcHandler с dispatch_fn (вместо Router&)
    auto handler = std::make_unique<GrpcHandler>(0, dispatch_fn);

    // 3. TxCoordinator с resume callback
    auto tx_coordinator = std::make_unique<TxCoordinator>(
      *routers[0],
      [&handler_ref = *handler](uint64_t rid, Task resp) {
        handler_ref.ResumeCoroutine(rid, std::move(resp));
      },
      wal_writers[0].get());

    auto coordinator_state = RecoveryManager::RecoverCoordinator(data_dir);
    if (!coordinator_state.tx_table.empty()) {
      tx_coordinator->LoadRecoveredState(
        std::move(coordinator_state.tx_table),
        coordinator_state.max_tx_id + 1,
        coordinator_state.max_snapshot_ts + 1);
    }

    // 4. CoreDispatcher — теперь все dependencies готовы
    auto dispatcher = std::make_unique<CoreDispatcher>(*routers[0], *handler, *tx_coordinator);

    // 5. Назначаем deferred pointer — dispatch_fn теперь работает
    // SAFETY: dispatch_fn вызывается только после старта event loop,
    // к этому моменту dispatcher_ptr уже валиден
    dispatcher_ptr = dispatcher.get();

    workers[0]->RegisterGrpcService(&handler->GetService());
    workers[0]->SetTaskProcessor([&dispatcher_ref = *dispatcher](Task task) {
      dispatcher_ref.Dispatch(std::move(task));
    });
    workers[0]->AddStartupTask([&worker = *workers[0], &handler_ref = *handler]() {
      handler_ref.RegisterHandlers(worker.GetGrpcContext());
    });

    for (int i = 1; i < cores; ++i) {
      workers[i]->SetTaskProcessor([&router = *routers[i]](Task task) {
        router.RouteTask(std::move(task));
      });
    }

    for (int i = 1; i < cores; ++i) {
      workers[i]->Start();
    }

    for (int i = 1; i < cores; ++i) {
      while (!workers[i]->IsReady()) {
        std::this_thread::yield();
      }
    }

    tx_coordinator->ResolveInDoubt(cores);

    RecoveryManager::WriteTopologyMeta(data_dir,
      {static_cast<uint32_t>(cores), layout_epoch});

    std::cout << "[Main] Recovery complete. Starting gRPC server.\n";

    // Периодический reaper + GC timer на Core 0
    auto reaper_timer = std::make_shared<boost::asio::steady_timer>(workers[0]->GetIoContext());
    std::function<void()> schedule_reap;

    schedule_reap = [reaper_timer, &tx_coordinator, &executors, &workers, cores,
                     &schedule_reap]() {
      reaper_timer->expires_after(std::chrono::seconds(1));
      reaper_timer->async_wait(
          [&, reaper_timer](const boost::system::error_code &ec) {
            if (ec) return;

            tx_coordinator->ReapStaleTransactions();

            uint64_t watermark = tx_coordinator->GetMinActiveSnapshot();
            if (watermark > 0 && watermark != UINT64_MAX) {
              Task gc0;
              gc0.type = TaskType::GC_REQUEST;
              gc0.snapshot_ts = watermark;
              executors[0]->Execute(std::move(gc0));
              for (int i = 1; i < cores; ++i) {
                Task gc_task;
                gc_task.type = TaskType::GC_REQUEST;
                gc_task.snapshot_ts = watermark;
                gc_task.reply_to_core = -1;
                workers[i]->PushTask(std::move(gc_task));
              }
            }

            schedule_reap();
          });
    };
    schedule_reap();

    std::cout << "[Main] Reaper timer started (1s interval).\n";

    boost::asio::signal_set signals(workers[0]->GetIoContext(), SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code &error, int signal_number) {
      if (!error) {
        std::cout << "\n[Core 0] Received OS signal " << signal_number
                  << ". Initiating graceful shutdown...\n";
        for (int i = 1; i < cores; ++i) {
          workers[i]->Stop();
        }
        workers[0]->Stop();
      }
    });

    std::cout << "[Main] Main thread is now transforming into Core 0...\n";
    workers[0]->StartSync();

    for (int i = 1; i < cores; ++i) {
      workers[i]->Join();
    }

    std::cout << "[Main] All cores successfully down. db_engine stopped.\n";

  } catch (std::exception &e) {
    std::cerr << "Fatal Error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
