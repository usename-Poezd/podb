#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>

#include "handlers/grpc_handler.h"
#include "router/router.h"
#include "storage/storage_engine.h"
#include "core/worker.h"
#include "core/core_dispatcher.h"

namespace po = boost::program_options;
using namespace db;

int main(int argc, char *argv[]) {
  try {
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Produce help message")("cores,c", po::value<int>(),
                                                         "Number of worker threads/cores")(
        "port,p", po::value<int>()->default_value(9906), "TCP port for gRPC");

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

    std::cout << "[Main] Starting STRICT Thread-per-Core engine.\n";
    std::cout << "[Main] Total cores: " << cores << ". Port: " << port << "\n";

    std::vector<std::unique_ptr<StorageEngine>> storages;
    storages.reserve(cores);
    for (int i = 0; i < cores; ++i) {
      storages.push_back(std::make_unique<StorageEngine>());
    }

    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(cores);
    for (int i = 0; i < cores; ++i) {
      workers.push_back(std::make_unique<Worker>(i, port));
    }

    std::vector<Worker *> worker_ptrs;
    worker_ptrs.reserve(cores);
    for (auto &w : workers) {
      worker_ptrs.push_back(w.get());
    }

    std::vector<std::unique_ptr<Router>> routers;
    std::vector<std::unique_ptr<GrpcHandler>> handlers;
    std::vector<std::unique_ptr<CoreDispatcher>> dispatchers;
    routers.reserve(cores);
    handlers.reserve(cores);
    dispatchers.reserve(cores);

    for (int i = 0; i < cores; ++i) {
      routers.push_back(std::make_unique<Router>(i, worker_ptrs, *storages[i]));

      handlers.push_back(std::make_unique<GrpcHandler>(i, *routers[i]));

      dispatchers.push_back(std::make_unique<CoreDispatcher>(*routers[i], *handlers[i]));

      workers[i]->RegisterGrpcService(&handlers[i]->GetService());

      workers[i]->SetTaskProcessor([&dispatcher = *dispatchers[i]](Task task) {
        dispatcher.Dispatch(std::move(task));
      });

      workers[i]->AddStartupTask([&worker = *workers[i], &handler = *handlers[i]]() {
        handler.RegisterHandlers(worker.GetGrpcContext());
      });
    }

    for (int i = 1; i < cores; ++i) {
      workers[i]->Start();
    }

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
