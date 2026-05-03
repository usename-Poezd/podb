#pragma once

#include <coroutine>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include <agrpc/grpc_context.hpp>
#include <agrpc/register_awaitable_rpc_handler.hpp>
#include <agrpc/server_rpc.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>

#include "api/service.grpc.pb.h"
#include "api/service.pb.h"
#include "async/request_tracker.h"
#include "core/task.h"
#include "handlers/proto_convert.h"

namespace db {

using GetRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestGet>;
using SetRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestSet>;
using BeginTxRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestBeginTransaction>;
using ExecuteRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestExecute>;
using CommitRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestCommit>;
using RollbackRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestRollback>;
using HeartbeatRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestHeartbeat>;

class GrpcHandler {
public:
  static constexpr size_t kMaxConcurrent = 65536;

  GrpcHandler(int core_id, std::function<void(Task)> dispatch_fn)
      : core_id_(core_id), dispatch_fn_(std::move(dispatch_fn)), tracker_(kMaxConcurrent) {}

  db::Database::AsyncService &GetService() { return service_; }

  void RegisterHandlers(agrpc::GrpcContext &grpc_ctx) {
    agrpc::register_awaitable_rpc_handler<GetRPC>(
        grpc_ctx, service_,
        [this](GetRPC &rpc, db::GetRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleGet(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<SetRPC>(
        grpc_ctx, service_,
        [this](SetRPC &rpc, db::SetRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleSet(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<BeginTxRPC>(
        grpc_ctx, service_,
        [this](BeginTxRPC &rpc, db::BeginTxRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleBeginTransaction(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<ExecuteRPC>(
        grpc_ctx, service_,
        [this](ExecuteRPC &rpc, db::ExecuteRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleExecute(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<CommitRPC>(
        grpc_ctx, service_,
        [this](CommitRPC &rpc, db::CommitRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleCommit(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<RollbackRPC>(
        grpc_ctx, service_,
        [this](RollbackRPC &rpc, db::RollbackRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleRollback(rpc, req);
        },
        boost::asio::detached);

    agrpc::register_awaitable_rpc_handler<HeartbeatRPC>(
        grpc_ctx, service_,
        [this](HeartbeatRPC &rpc, db::HeartbeatRequest &req) -> boost::asio::awaitable<void> {
          co_return co_await HandleHeartbeat(rpc, req);
        },
        boost::asio::detached);
  }

  void ResumeCoroutine(uint64_t request_id, Task response) {
    tracker_.Fulfill(request_id, std::move(response));
  }

private:
  boost::asio::awaitable<Task> WaitForResponse(Task task) {

    co_return co_await boost::asio::async_initiate<
        const boost::asio::use_awaitable_t<boost::asio::any_io_executor>, void(Task)>(
        [this, task = std::move(task)](auto &&completion_handler) mutable {
          using Handler = std::decay_t<decltype(completion_handler)>;

          auto shared_ch = std::make_shared<Handler>(std::move(completion_handler));

          Task routed_task = std::move(task);
          uint64_t rid = tracker_.AllocSlot();
          uint32_t index = static_cast<uint32_t>(rid & 0xFFFFFFFF);
          routed_task.request_id = rid;
          routed_task.reply_to_core = core_id_;

          tracker_.SetCompletion(index, [this, index, shared_ch]() {
            auto resp = tracker_.GetResponse(index);
            (*shared_ch)(std::move(resp));
          });

          dispatch_fn_(std::move(routed_task));
        },
        boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleGet(GetRPC &rpc, db::GetRequest &req) {
    std::printf("[Core %d] >>> GET  \"%.20s\"\n", core_id_, req.key().c_str());
    Task task;
    task.type = TaskType::GET_REQUEST;
    task.key = req.key();
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< GET  \"%.20s\" found=%s\n", core_id_, response.key.c_str(),
                response.found ? "yes" : "no");
    db::GetResponse grpc_resp;
    grpc_resp.set_found(response.found);
    if (response.found)
      grpc_resp.set_value(ToProtoBytes(response.value));
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleSet(SetRPC &rpc, db::SetRequest &req) {
    std::printf("[Core %d] >>> SET  \"%.20s\" size=%zu\n", core_id_, req.key().c_str(),
                req.value().size());
    Task task;
    task.type = TaskType::SET_REQUEST;
    task.key = req.key();
    task.value = FromProtoBytes(req.value());
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< SET  \"%.20s\" ok=%s\n", core_id_, response.key.c_str(),
                response.success ? "yes" : "no");
    db::SetResponse grpc_resp;
    grpc_resp.set_success(response.success);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleBeginTransaction(BeginTxRPC &rpc, db::BeginTxRequest &req) {
    std::printf("[Core %d] >>> BEGIN_TX isolation=\"%s\"\n", core_id_, req.isolation_level().c_str());
    Task task;
    task.type = TaskType::TX_BEGIN_REQUEST;
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< BEGIN_TX tx_id=%lu success=%s\n", core_id_, response.tx_id,
                response.success ? "yes" : "no");
    db::BeginTxResponse grpc_resp;
    grpc_resp.set_tx_id(response.tx_id);
    grpc_resp.set_snapshot_ts(response.snapshot_ts);
    grpc_resp.set_success(response.success);
    if (!response.error_message.empty())
      grpc_resp.set_error(response.error_message);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleExecute(ExecuteRPC &rpc, db::ExecuteRequest &req) {
    std::printf("[Core %d] >>> EXECUTE tx_id=%lu op=\"%s\" key=\"%.20s\"\n", core_id_,
                req.tx_id(), req.operation().c_str(), req.key().c_str());
    Task task;
    if (req.operation() == "GET") {
      task.type = TaskType::TX_EXECUTE_GET_REQUEST;
    } else if (req.operation() == "SET") {
      task.type = TaskType::TX_EXECUTE_SET_REQUEST;
    } else {
      // Невалидная операция — возвращаем ошибку без маршрутизации
      db::ExecuteResponse grpc_resp;
      grpc_resp.set_tx_id(req.tx_id());
      grpc_resp.set_success(false);
      grpc_resp.set_error("invalid_operation: expected GET or SET");
      co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
      co_return;
    }
    task.tx_id = req.tx_id();
    task.key = req.key();
    task.value = FromProtoBytes(req.value());
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< EXECUTE tx_id=%lu success=%s\n", core_id_, response.tx_id,
                response.success ? "yes" : "no");
    db::ExecuteResponse grpc_resp;
    grpc_resp.set_tx_id(response.tx_id);
    grpc_resp.set_found(response.found);
    if (response.found)
      grpc_resp.set_value(ToProtoBytes(response.value));
    grpc_resp.set_success(response.success);
    if (!response.error_message.empty())
      grpc_resp.set_error(response.error_message);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleCommit(CommitRPC &rpc, db::CommitRequest &req) {
    std::printf("[Core %d] >>> COMMIT tx_id=%lu\n", core_id_, req.tx_id());
    Task task;
    task.type = TaskType::TX_COMMIT_REQUEST;
    task.tx_id = req.tx_id();
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< COMMIT tx_id=%lu success=%s\n", core_id_, response.tx_id,
                response.success ? "yes" : "no");
    db::CommitResponse grpc_resp;
    grpc_resp.set_tx_id(response.tx_id);
    grpc_resp.set_success(response.success);
    if (!response.error_message.empty())
      grpc_resp.set_error(response.error_message);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleRollback(RollbackRPC &rpc, db::RollbackRequest &req) {
    std::printf("[Core %d] >>> ROLLBACK tx_id=%lu\n", core_id_, req.tx_id());
    Task task;
    task.type = TaskType::TX_ROLLBACK_REQUEST;
    task.tx_id = req.tx_id();
    Task response = co_await WaitForResponse(std::move(task));
    std::printf("[Core %d] <<< ROLLBACK tx_id=%lu success=%s\n", core_id_, response.tx_id,
                response.success ? "yes" : "no");
    db::RollbackResponse grpc_resp;
    grpc_resp.set_tx_id(response.tx_id);
    grpc_resp.set_success(response.success);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> HandleHeartbeat(HeartbeatRPC &rpc, db::HeartbeatRequest &req) {
    std::printf("[Core %d] >>> HEARTBEAT tx_id=%lu\n", core_id_, req.tx_id());
    Task task;
    task.type = TaskType::TX_HEARTBEAT_REQUEST;
    task.tx_id = req.tx_id();
    Task response = co_await WaitForResponse(std::move(task));
    db::HeartbeatResponse grpc_resp;
    grpc_resp.set_tx_id(response.tx_id);
    grpc_resp.set_alive(response.success);
    co_await rpc.finish(grpc_resp, grpc::Status::OK, boost::asio::use_awaitable);
  }

  int core_id_;
  db::Database::AsyncService service_;
  std::function<void(Task)> dispatch_fn_;
  RequestTracker tracker_;
};

}  // namespace db
