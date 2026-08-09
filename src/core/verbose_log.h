#pragma once

#include <cstdio>

namespace db {

/// Включён/выключен через --verbose при старте db_engine (main.cpp), до
/// запуска worker-потоков. После этого только читается разными потоками —
/// т.к. запись происходит строго до их создания, отдельная синхронизация
/// не нужна (happens-before через std::thread::start()).
inline bool g_verbose_logging = false;

}  // namespace db

/// Логирование per-request debug-трейсов (>>> / <<< / ROUT / EXEC / ...).
/// При выключенном g_verbose_logging аргументы даже не вычисляются (в
/// отличие от обёртки-функции) — единственная цена в hot path — одна
/// проверка bool. perf показал ~3% self-time Core 0 уходило в сам printf,
/// плюс ещё в цепочку write()-syscall в его call-graph.
#define PODB_VLOG(...)             \
  do {                             \
    if (::db::g_verbose_logging) { \
      std::printf(__VA_ARGS__);    \
    }                              \
  } while (0)
