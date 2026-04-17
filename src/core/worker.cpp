#include "core/worker.h"

#include <agrpc/run.hpp>
#include <boost/asio/buffer.hpp>
#include <cstdint>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <iostream>
#include <pthread.h>
#include <sched.h>
#include <stdexcept>
#include <string>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

using namespace db;

Worker::Worker(int core_id, int port)
    : core_id_(core_id), port_(port), event_fd_(-1), event_fd_stream_(io_context_) {}

Worker::~Worker() {
  Stop();
  if (event_fd_ != -1) {
    close(event_fd_);
  }
}

void Worker::RegisterGrpcService(grpc::Service *service) { builder_.RegisterService(service); }

void Worker::SetTaskProcessor(std::function<void(Task)> processor) {
  task_processor_ = std::move(processor);
}

void Worker::AddStartupTask(std::function<void()> task) {
  startup_tasks_.push_back(std::move(task));
}

agrpc::GrpcContext &Worker::GetGrpcContext() { return grpc_context_.value(); }

void Worker::Start() {
  running_ = true;
  thread_ = std::thread(&Worker::RunLoop, this);
}
void Worker::StartSync() {
  running_ = true;
  RunLoop();
}
void Worker::Stop() {
  running_ = false;
  io_context_.stop();
  if (server_) {
    server_->Shutdown();
    if (grpc_context_)
      grpc_context_->stop();
  }
  if (event_fd_ != -1) {
    uint64_t wake_up = 1;
    if (write(event_fd_, &wake_up, sizeof(wake_up)) == -1) {
      std::cerr << "[Core " << core_id_ << "] Warning: stop eventfd write failed\n";
    }
  }
}

void Worker::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Worker::PushTask(Task task) {
  task_queue_.enqueue(std::move(task));
  uint64_t msg = 1;
  if (write(event_fd_, &msg, sizeof(msg)) == -1) {
    std::cerr << "[Core " << core_id_ << "] Warning: eventfd write failed\n";
  }
}

void Worker::SetCpuAffinity() {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id_, &cpuset);

  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "[Core " << core_id_ << "] Failed to set CPU affinity\n";
  } else {
    std::cout << "[Core " << core_id_ << "] Successfully pinned to hardware core\n";
  }
}

void Worker::BuildAndStartGrpcServer() {
  const std::string server_address = "0.0.0.0:" + std::to_string(port_);

  builder_.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
  builder_.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  auto grpc_cq = builder_.AddCompletionQueue();
  server_ = builder_.BuildAndStart();
  grpc_context_.emplace(std::move(grpc_cq));

  std::cout << "[Core " << core_id_ << "] gRPC Server listening on " << server_address
            << " (SO_REUSEPORT)\n";
}

void Worker::InitEventFd() {
  event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
  if (event_fd_ == -1) {
    throw std::runtime_error("Failed to create eventfd");
  }
  event_fd_stream_.assign(event_fd_);
}

void Worker::StartEventFdRead() {
  event_fd_stream_.async_read_some(boost::asio::buffer(&event_fd_buffer_, sizeof(event_fd_buffer_)),
                                   [this](const boost::system::error_code &error, std::size_t) {
                                     if (!error && running_) {
                                       Task task{};
                                       while (task_queue_.try_dequeue(task)) {
                                         if (task_processor_) {
                                           task_processor_(std::move(task));
                                         }
                                       }
                                       StartEventFdRead();
                                     }
                                   });
}

void Worker::RunLoop() {
  SetCpuAffinity();
  InitEventFd();
  StartEventFdRead();
  BuildAndStartGrpcServer();

  for (auto &init_task : startup_tasks_) {
    init_task();
  }

  agrpc::run(*grpc_context_, io_context_);
}
