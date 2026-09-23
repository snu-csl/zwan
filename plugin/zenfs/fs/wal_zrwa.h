// WALTZ ZRWA WAL: write-ahead log on the Zone Random Write Area.
// Log records are packed into 4K pages inside the ZRWA window.

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "spdk/nvme.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class WalZrwa;

// extent tracking info returned by appendRecord
struct ZrwaAppendResult {
  uint64_t start_pos;        // first written page byte address
  uint64_t end_pos;          // byte address past last written page
  Zone* zone;                // active zone at write time
  uint32_t zone_generation;  // generation at write time
};

// free ring: the IO thread pushes, worker threads pop via CAS
struct WalRingEntry {
  uint32_t slot_idx{0};
  uint64_t expected_logical_page{UINT64_MAX};
  uint32_t zone_generation{0};
};

struct WalFreeRing {
  alignas(64) std::atomic<uint32_t> write_pos{0};
  alignas(64) std::atomic<uint32_t> read_pos{0};
  std::vector<WalRingEntry> entries;
  uint32_t capacity{0};

  void init(uint32_t cap);
  void reset();
  void push(const WalRingEntry& entry);
  bool pop(WalRingEntry& entry_out);
};

// per-ZRWA-page state
struct WalPageSlot {
  static constexpr uint8_t BUF_FREE = 0;
  static constexpr uint8_t BUF_INFLIGHT = 1;
  static constexpr uint8_t BUF_SEALED = 2;
  static constexpr uint8_t BUF_READY = 3;

  enum class SealState : uint8_t { NONE = 0, PENDING = 1, IN_PROGRESS = 2 };

  uint32_t dma_idx;
  std::atomic<uint8_t> buf_state{BUF_FREE};
  std::atomic<SealState> seal_state{SealState::NONE};
  std::atomic<uint64_t> logical_page{UINT64_MAX};
  uint16_t free_offset{0};
  uint32_t page_crc{0};
  bool device_bytes_counted{false};

  WalPageSlot() = default;
  WalPageSlot(WalPageSlot&& o) noexcept
      : dma_idx(o.dma_idx),
        buf_state(o.buf_state.load(std::memory_order_relaxed)),
        seal_state(o.seal_state.load(std::memory_order_relaxed)),
        logical_page(o.logical_page.load(std::memory_order_relaxed)),
        free_offset(o.free_offset),
        page_crc(o.page_crc),
        device_bytes_counted(o.device_bytes_counted) {}
  WalPageSlot& operator=(WalPageSlot&& o) noexcept {
    dma_idx = o.dma_idx;
    buf_state.store(o.buf_state.load(std::memory_order_relaxed), std::memory_order_relaxed);
    seal_state.store(o.seal_state.load(std::memory_order_relaxed), std::memory_order_relaxed);
    logical_page.store(o.logical_page.load(std::memory_order_relaxed), std::memory_order_relaxed);
    free_offset = o.free_offset;
    page_crc = o.page_crc;
    device_bytes_counted = o.device_bytes_counted;
    return *this;
  }

  bool trySetPending() {
    auto expected = SealState::NONE;
    return seal_state.compare_exchange_strong(expected, SealState::PENDING,
        std::memory_order_acq_rel, std::memory_order_acquire);
  }
  bool tryEnterInProgress() {
    auto expected = SealState::PENDING;
    return seal_state.compare_exchange_strong(expected, SealState::IN_PROGRESS,
        std::memory_order_acq_rel, std::memory_order_acquire);
  }
  void finishSeal() {
    seal_state.store(SealState::NONE, std::memory_order_release);
  }
};

// zone transition phases
enum class ZoneFullPhase : uint8_t {
  NONE = 0, DRAINING = 1
};

// dedicated IO thread for ZRWA WAL writes
class WalZrwaIoThread {
 public:
  struct WriteCtx {
    uint32_t slot_idx;
    uint64_t logical_page;
    WalZrwaIoThread* io_thread{nullptr};
    uint64_t submit_tsc{0};
    std::atomic<bool>* done_flag{nullptr};
  };

  WalZrwaIoThread() = default;
  ~WalZrwaIoThread();

  bool init(WalZrwa* zrwa);
  void start();
  void stop();
  void shutdown();

  void markCompleted(uint64_t logical_page);
  void processSealedSlots();
  void refillSlots();
  void processZoneFullPhase();
  void resetFlushPendingWp() { flush_pending_wp_ = 0; }

  std::unique_ptr<std::atomic<uint64_t>[]>& getCompletedWords() {
    return completed_words_;
  }
  uint32_t getCompletedNumWords() { return completed_num_words_; }
  uint64_t getFlushPendingWp() { return flush_pending_wp_; }

  std::vector<WriteCtx>& getWriteCtxPool() { return write_ctx_pool_; }

 private:
  enum class RetirePhase : uint8_t {
    IDLE = 0,
    CLOSE_SUBMITTED = 1,
    FINISH_SUBMITTED = 2,
    COMPLETE = 3,
  };

  void ioLoop();
  int scanAndSubmitWrites();
  void checkAdvance();
  void sendFlush(uint64_t count);
  bool submitRetireClose(Zone* zone);
  void completeRetireFinish(Zone* zone);

  static void writeCompleteCb(void* arg, const struct spdk_nvme_cpl* cpl);
  static void flushCompleteCb(void* arg, const struct spdk_nvme_cpl* cpl);
  static void retireFinishCompleteCb(void* arg, const struct spdk_nvme_cpl* cpl);

  WalZrwa* zrwa_{nullptr};

  spdk_nvme_qpair* write_qpair_{nullptr};
  spdk_nvme_qpair* mgmt_qpair_{nullptr};

  std::vector<WriteCtx> write_ctx_pool_;

  std::unique_ptr<std::atomic<uint64_t>[]> completed_words_;
  uint32_t completed_num_words_{0};

  uint64_t flush_pending_wp_{0};

  std::thread thread_;
  std::atomic<bool> running_{false};

  struct FlushCtx {
    WalZrwaIoThread* self;
    uint64_t count;
    uint64_t target_wp;
    uint64_t zone_slba;
  };

  struct RetireCtx {
    WalZrwaIoThread* self;
    Zone* zone;
    uint64_t zone_slba;
  };

  Zone* replacement_zone_{nullptr};
  RetirePhase retire_phase_{RetirePhase::IDLE};

  // false while a retiring zone's finish is in flight, the allocation thread
  // defers WAL queue refill until then
  std::atomic<bool> wal_finish_complete_{true};

 public:
  bool isWalFinishComplete() const {
    return wal_finish_complete_.load(std::memory_order_acquire);
  }
};

class WalZrwa {
 public:
  static constexpr uint32_t WAL_PAGE_SIZE = 4096;
  static constexpr uint32_t LOG_HEADER_SIZE = 3;     // len(2)+type(1)
  static constexpr uint32_t PAGE_HEADER_SIZE = 4;   // per-page CRC(4)
  static constexpr uint32_t MIN_FLUSH_PAGES = 8;

  struct Stats {
    std::atomic<uint64_t> nvme_write_count{0};    // NVMe write commands issued
    std::atomic<uint64_t> nvme_write_lat_sum{0};  // Sum of write latency (TSC ticks)
    std::atomic<uint64_t> flush_count{0};         // ZRWA flush commands issued
    std::atomic<uint64_t> payload_bytes{0};       // Actual WAL payload bytes
    std::atomic<uint64_t> device_bytes{0};        // 4 KiB charged once per logical page slot
    std::atomic<uint64_t> submit_bytes{0};        // 4 KiB charged on every page submit to device
    std::atomic<uint64_t> submit_first_tsc{0};    // First successful page submit TSC
    std::atomic<uint64_t> submit_last_tsc{0};     // Last successful page submit TSC
    std::atomic<uint64_t> submit_window_start_tsc{0};  // Current peak bucket start
    std::atomic<uint64_t> submit_window_bytes{0};      // Bytes in current peak bucket
    std::atomic<uint64_t> submit_peak_bps{0};     // Peak submit throughput in bytes/sec
    std::atomic<uint64_t> append_count{0};        // appendRecord calls
    std::atomic<uint64_t> append_lat_sum{0};      // Sum of appendRecord latency (TSC)
    std::atomic<uint64_t> page_used_bytes_sum{0}; // Sum of used bytes per page (for avg)
    std::atomic<uint64_t> sync_notify_count{0};   // done_flag notifications fired
  };
  Stats stats;

  WalZrwa();
  ~WalZrwa();

  bool init(ZonedBlockDevice* zbd);
  void shutdown();
  void simulateCrash();
  void printStats(const char* phase = nullptr);
  void resetStats();
  void recordDevicePageSubmit(uint64_t submit_tsc);

  // appends one WAL record from the writer thread, a record larger than a
  // page is not supported by extent tracking
  IOStatus appendRecord(const char* payload, size_t payload_size,
                        ZrwaAppendResult* result);

  Zone* getActiveZone() { return active_zone_; }

  uint32_t getZrwaSize() const { return zrwa_size_; }
  uint32_t getZrwaFg() const { return zrwafg_; }
  uint64_t getZoneSlba() const { return zone_slba_; }
  std::atomic<uint64_t>& getWpPage() { return wp_page_; }
  std::atomic<uint64_t>& getTail() { return tail_; }
  std::atomic<uint32_t>& getPendingWrites() { return pending_writes_; }
  WalPageSlot& getSlot(uint32_t idx) { return page_slots_[idx]; }
  uint8_t* getDmaPage(uint32_t idx) { return dma_pages_[idx]; }
  std::atomic<uint8_t>& getReadyFlag(uint32_t idx) { return ready_flags_[idx]; }
  std::atomic<bool>& getTransitioning() { return zone_transitioning_; }
  ZonedBlockDevice* getZbd() { return zbd_; }
  WalZrwaIoThread* getIoThread() { return io_thread_; }

  ZoneFullPhase getZoneFullPhase() { return zone_full_phase_; }
  void setZoneFullPhase(ZoneFullPhase p) { zone_full_phase_ = p; }
  uint64_t getZoneCapacityPages() { return zone_capacity_pages_; }
  void setActiveZone(Zone* z) { active_zone_ = z; }

  void initZoneState();
  bool openZoneZrwaSync(Zone* zone);
  void pushToRing(uint32_t slot_idx);

 private:
  ZonedBlockDevice* zbd_{nullptr};
  WalZrwaIoThread* io_thread_{nullptr};

  Zone* active_zone_{nullptr};
  uint64_t zone_slba_{0};
  std::atomic<uint64_t> wp_page_{0};
  std::atomic<uint64_t> tail_{0};
  uint64_t zone_capacity_pages_{0};

  // ZRWA device parameters
  uint32_t zrwa_size_{0};
  uint32_t zrwafg_{0};
  bool zrwa_supported_{false};

  uint32_t blk_sft_{0};

  std::vector<WalPageSlot> page_slots_;
  std::vector<uint8_t*> dma_pages_;
  std::unique_ptr<std::atomic<uint8_t>[]> ready_flags_;

  WalFreeRing normal_ring_;
  WalFreeRing prio_ring_;
  uint32_t prio_window_{0};
  std::atomic<uint32_t> zone_generation_{0};
  std::atomic<bool> shutdown_called_{false};
  std::atomic<bool> phase_reset_used_{false};

  std::atomic<uint32_t> pending_writes_{0};

  std::atomic<bool> zone_transitioning_{false};
  ZoneFullPhase zone_full_phase_{ZoneFullPhase::NONE};

  bool readZrwaParams();
  bool allocateSlots();
  bool openZoneZrwa(Zone* zone, spdk_nvme_qpair* qpair);
  bool acquireSlot(uint32_t& slot_idx, uint16_t& avail);
  void sealAndRelease(uint32_t slot_idx);
  void writeAndRelease(uint32_t slot_idx);
};

// null when the WAL runs in append mode
extern WalZrwa* g_wal_zrwa;

}  // namespace ROCKSDB_NAMESPACE

// outside the namespace for C linkage
extern std::string waltz_wal_mode;
extern uint32_t waltz_prio_window;

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
