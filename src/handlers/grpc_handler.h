#pragma once

#include <coroutine>
#include <cstdio>
#include <cstddef>
#include <cstdint>
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

#include "router/router.h"
#include "api/service.grpc.pb.h"
#include "api/service.pb.h"
#include "async/request_tracker.h"
#include "core/task.h"
#include "handlers/proto_convert.h"

namespace db {

using GetRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestGet>;
using SetRPC = agrpc::ServerRPC<&db::Database::AsyncService::RequestSet>;

class GrpcHandler {
public:
  static constexpr size_t kMaxConcurrent = 65536;

  GrpcHandler(int core_id, Router &router)
      : core_id_(core_id), router_(router), tracker_(kMaxConcurrent) {}

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

          router_.RouteTask(std::move(routed_task));
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

  int core_id_;
  db::Database::AsyncService service_;
  Router &router_;
  RequestTracker tracker_;
};

}  // namespace db
