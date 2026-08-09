#include "worker.h"

#include <chrono>
#include <memory>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "api/service.grpc.pb.h"
#include "workload.h"

namespace bench {

namespace {

std::unique_ptr<db::Database::Stub> MakeStub(const std::string &address) {
  // A load-test client must own its retry semantics explicitly — gRPC's
  // built-in transport retries would otherwise duplicate a call underneath
  // our own write_write_conflict retry loop and silently corrupt both the
  // latency samples and the retry count.
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
  auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
  return db::Database::NewStub(channel);
}

void SetDeadline(grpc::ClientContext &ctx, int timeout_ms) {
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
}

uint32_t ElapsedUs(std::chrono::steady_clock::time_point start) {
  return static_cast<uint32_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
}

// stats == nullptr means "warmup": issue the call but discard every sample.
void DoOneKvOp(db::Database::Stub &stub, Workload &workload, const Options &opts, ThreadStats *stats) {
  const bool is_read = workload.NextIsRead();
  const std::string key = workload.NextKey();

  grpc::ClientContext ctx;
  SetDeadline(ctx, opts.rpc_timeout_ms);

  const auto start = std::chrono::steady_clock::now();
  grpc::Status status;
  bool app_ok = true;
  if (is_read) {
    db::GetRequest req;
    req.set_key(key);
    db::GetResponse resp;
    status = stub.Get(&ctx, req, &resp);
  } else {
    db::SetRequest req;
    req.set_key(key);
    req.set_value(workload.ValueBuffer());
    db::SetResponse resp;
    status = stub.Set(&ctx, req, &resp);
    app_ok = resp.success();
  }
  const uint32_t elapsed_us = ElapsedUs(start);

  if (stats == nullptr) return;

  if (!status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) stats->timeout_count++;
    if (is_read) {
      stats->get_rpc_err++;
    } else {
      stats->set_rpc_err++;
    }
    return;
  }
  if (is_read) {
    stats->get_ok++;
    stats->get_us.push_back(elapsed_us);
  } else if (app_ok) {
    stats->set_ok++;
    stats->set_us.push_back(elapsed_us);
  } else {
    stats->set_app_err++;
  }
}

void DoOneTxCycle(db::Database::Stub &stub, Workload &workload, const Options &opts, ThreadStats *stats) {
  const auto cycle_start = std::chrono::steady_clock::now();

  db::BeginTxRequest begin_req;
  begin_req.set_priority(opts.tx_priority);
  db::BeginTxResponse begin_resp;
  {
    grpc::ClientContext ctx;
    SetDeadline(ctx, opts.rpc_timeout_ms);
    auto status = stub.BeginTransaction(&ctx, begin_req, &begin_resp);
    if (!status.ok() || !begin_resp.success()) {
      if (stats) stats->tx_other_abort++;
      return;
    }
  }
  const uint64_t tx_id = begin_resp.tx_id();

  bool conflict_aborted = false;
  bool other_aborted = false;

  for (int i = 0; i < opts.tx_ops && !conflict_aborted && !other_aborted; ++i) {
    const bool is_read = workload.NextIsRead();
    const std::string key = workload.NextKey();
    int retries = 0;

    for (;;) {
      db::ExecuteRequest req;
      req.set_tx_id(tx_id);
      req.set_operation(is_read ? "GET" : "SET");
      req.set_key(key);
      if (!is_read) req.set_value(workload.ValueBuffer());
      db::ExecuteResponse resp;

      grpc::ClientContext ctx;
      SetDeadline(ctx, opts.rpc_timeout_ms);
      const auto exec_start = std::chrono::steady_clock::now();
      auto status = stub.Execute(&ctx, req, &resp);
      const uint32_t exec_us = ElapsedUs(exec_start);
      if (stats) stats->tx_execute_us.push_back(exec_us);

      if (!status.ok()) {
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED && stats) stats->timeout_count++;
        other_aborted = true;
        break;
      }
      if (resp.success()) break;

      if (resp.error() == "write_write_conflict" && retries < opts.max_retries) {
        if (stats) stats->retry_count++;
        ++retries;
        if (opts.honor_retry_after && resp.retry_after_ms() > 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(resp.retry_after_ms()));
        }
        continue;
      }
      if (resp.error() == "write_write_conflict") {
        conflict_aborted = true;
      } else {
        other_aborted = true;
      }
      break;
    }
  }

  bool committed = false;
  if (!conflict_aborted && !other_aborted) {
    db::CommitRequest req;
    req.set_tx_id(tx_id);
    db::CommitResponse resp;
    grpc::ClientContext ctx;
    SetDeadline(ctx, opts.rpc_timeout_ms);
    const auto commit_start = std::chrono::steady_clock::now();
    auto status = stub.Commit(&ctx, req, &resp);
    const uint32_t commit_us = ElapsedUs(commit_start);
    if (status.ok() && resp.success()) {
      committed = true;
      if (stats) stats->commit_us.push_back(commit_us);
    } else {
      other_aborted = true;
    }
  } else {
    db::RollbackRequest req;
    req.set_tx_id(tx_id);
    db::RollbackResponse resp;
    grpc::ClientContext ctx;
    SetDeadline(ctx, opts.rpc_timeout_ms);
    stub.Rollback(&ctx, req, &resp);
  }

  if (stats == nullptr) return;

  const uint32_t cycle_us = ElapsedUs(cycle_start);
  stats->tx_cycle_all_us.push_back(cycle_us);
  if (committed) {
    stats->tx_committed++;
    stats->tx_cycle_committed_us.push_back(cycle_us);
  } else if (conflict_aborted) {
    stats->tx_conflict_abort++;
  } else {
    stats->tx_other_abort++;
  }
}

}  // namespace

void RunKvWorker(const Options &opts, const RunControl &control, ThreadStats &stats) {
  auto stub = MakeStub(opts.address);
  Workload workload(opts.keyspace, opts.value_size, opts.read_ratio);

  while (std::chrono::steady_clock::now() < control.warmup_end) {
    DoOneKvOp(*stub, workload, opts, nullptr);
  }

  for (;;) {
    if (control.remaining_requests != nullptr) {
      if (control.remaining_requests->fetch_sub(1, std::memory_order_relaxed) <= 0) break;
    } else if (std::chrono::steady_clock::now() >= control.measure_end) {
      break;
    }
    DoOneKvOp(*stub, workload, opts, &stats);
  }
}

void RunTxWorker(const Options &opts, const RunControl &control, ThreadStats &stats) {
  auto stub = MakeStub(opts.address);
  Workload workload(opts.keyspace, opts.value_size, opts.read_ratio);

  while (std::chrono::steady_clock::now() < control.warmup_end) {
    DoOneTxCycle(*stub, workload, opts, nullptr);
  }

  for (;;) {
    if (control.remaining_requests != nullptr) {
      if (control.remaining_requests->fetch_sub(1, std::memory_order_relaxed) <= 0) break;
    } else if (std::chrono::steady_clock::now() >= control.measure_end) {
      break;
    }
    DoOneTxCycle(*stub, workload, opts, &stats);
  }
}

}  // namespace bench
