#include "monitoring/waf_stats.h"

#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <string>

extern bool waltz_mode;

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)
extern std::string waltz_wal_mode;
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

std::atomic<uint64_t> g_user_data_bytes{0};
std::atomic<uint64_t> g_wal_payload_bytes{0};
std::atomic<uint64_t> g_wal_record_count{0};
std::atomic<uint64_t> g_wal_device_bytes{0};
std::atomic<uint64_t> g_total_device_bytes{0};
std::atomic<uint64_t> g_meta_device_bytes{0};

const char* DetectWafMode() {
  if (!waltz_mode) {
    return "zenfs";
  }
#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)
  if (waltz_wal_mode == "zrwa") {
    return "zrwa";
  }
#endif
  return "append";
}

double SafeRatio(uint64_t numerator, uint64_t denominator) {
  if (denominator == 0) {
    return 0.0;
  }
  return static_cast<double>(numerator) / static_cast<double>(denominator);
}

}  // namespace

void ResetWafStats() {
  g_user_data_bytes.store(0, std::memory_order_relaxed);
  g_wal_payload_bytes.store(0, std::memory_order_relaxed);
  g_wal_record_count.store(0, std::memory_order_relaxed);
  g_wal_device_bytes.store(0, std::memory_order_relaxed);
  g_total_device_bytes.store(0, std::memory_order_relaxed);
  g_meta_device_bytes.store(0, std::memory_order_relaxed);
}

void AddUserDataBytes(uint64_t bytes) {
  g_user_data_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void AddWalPayloadBytes(uint64_t bytes) {
  g_wal_payload_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void AddWalRecordCount(uint64_t count) {
  g_wal_record_count.fetch_add(count, std::memory_order_relaxed);
}

void AddWalDeviceBytes(uint64_t bytes) {
  g_wal_device_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void AddTotalDeviceBytes(uint64_t bytes) {
  g_total_device_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void AddMetaDeviceBytes(uint64_t bytes) {
  g_meta_device_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void PrintWafStats(const char* phase) {
  const uint64_t user_data_bytes =
      g_user_data_bytes.load(std::memory_order_relaxed);
  const uint64_t wal_payload_bytes =
      g_wal_payload_bytes.load(std::memory_order_relaxed);
  const uint64_t wal_record_count =
      g_wal_record_count.load(std::memory_order_relaxed);
  const uint64_t wal_device_bytes =
      g_wal_device_bytes.load(std::memory_order_relaxed);
  const uint64_t total_device_bytes =
      g_total_device_bytes.load(std::memory_order_relaxed);
  const uint64_t meta_device_bytes =
      g_meta_device_bytes.load(std::memory_order_relaxed);

  fprintf(stderr, "===== WAF Stats =====\n");
  fprintf(stderr, "Mode: %s\n", DetectWafMode());
  fprintf(stderr, "Phase: %s\n", phase != nullptr ? phase : "unknown");
  fprintf(stderr, "User data bytes: %" PRIu64 "\n", user_data_bytes);
  fprintf(stderr, "WAL payload bytes: %" PRIu64 "\n", wal_payload_bytes);
  fprintf(stderr, "WAL record count: %" PRIu64 "\n", wal_record_count);
  fprintf(stderr, "WAL device bytes: %" PRIu64 "\n", wal_device_bytes);
  fprintf(stderr, "Total device bytes: %" PRIu64 "\n", total_device_bytes);
  fprintf(stderr, "Meta device bytes: %" PRIu64 "\n", meta_device_bytes);
  fprintf(stderr, "WAL WAF: %.6f\n",
          SafeRatio(wal_device_bytes, user_data_bytes));
  fprintf(stderr, "Total WAF: %.6f\n",
          SafeRatio(total_device_bytes, user_data_bytes));
  fprintf(stderr, "Meta WAF: %.6f\n",
          SafeRatio(meta_device_bytes, user_data_bytes));

  fprintf(stderr, "=====================\n");
  fflush(stderr);
}

}  // namespace ROCKSDB_NAMESPACE
