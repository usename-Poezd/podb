#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sys/eventfd.h>

#include <agrpc/asio_grpc.hpp>
#include <agrpc/grpc_context.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <moodycamel/concurrentqueue.h>

#include "core/task.h"

namespace db {

class alignas(64) Worker {
public:
  Worker(int core_id, int port);
  ~Worker();

  Worker(const Worker &) = delete;
  Worker &operator=(const Worker &) = delete;
  Worker(Worker &&) = delete;
  Worker &operator=(Worker &&) = delete;

  // Регистрирует gRPC-сервис в builder'е этого воркера.
  void RegisterGrpcService(grpc::Service *service);

  // Устанавливает обработчик входящих задач из очереди.
  // Вызывается при каждом сигнале eventfd с задачами из task_queue_.
  void SetTaskProcessor(std::function<void(Task)> processor);

  // Добавляет задачу инициализации, которая запустится сразу после
  // BuildAndStartGrpcServer() внутри RunLoop() — когда grpc_context_ уже готов.
  void AddStartupTask(std::function<void()> task);

  agrpc::GrpcContext &GetGrpcContext();
  boost::asio::io_context &GetIoContext() { return io_context_; }

  void PushTask(Task task);

  void Start();
  void StartSync();
  void Stop();
  void Join();
  void RunLoop();

private:
  void SetCpuAffinity();
  void InitEventFd();
  void StartEventFdRead();
  void BuildAndStartGrpcServer();

  int core_id_;
  int port_;
  std::atomic<bool> running_{false};
  std::thread thread_;

  boost::asio::io_context io_context_;

  grpc::ServerBuilder builder_;
  std::unique_ptr<grpc::Server> server_;
  std::optional<agrpc::GrpcContext> grpc_context_;

  moodycamel::ConcurrentQueue<Task> task_queue_;

  int event_fd_;
  boost::asio::posix::stream_descriptor event_fd_stream_;
  uint64_t event_fd_buffer_{0};

  std::function<void(Task)> task_processor_;
  std::vector<std::function<void()>> startup_tasks_;
};

} // namespace db
