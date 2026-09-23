// WALTZ ZRWA WAL implementation, based on the leanstore-zc BufferZone design.

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "wal_zrwa.h"

#include <cassert>
#include <cstring>
#include <emmintrin.h>

#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"

#include "db/log_format.h"
#include "monitoring/waf_stats.h"
#include "util/coding.h"
#include "util/crc32c.h"

extern bool zrwa_exp_flush;
std::string waltz_wal_mode = "append";
uint32_t waltz_prio_window = 4;

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr double kTscGhz = 2.2;
constexpr double kBytesPerMiB = 1024.0 * 1024.0;
constexpr uint64_t kPeakSubmitWindowUs = 1000;

double TicksToSeconds(uint64_t ticks) {
  return static_cast<double>(ticks) / (kTscGhz * 1000.0 * 1000.0 * 1000.0);
}

uint64_t ComputeBytesPerSecond(uint64_t bytes, uint64_t ticks) {
  if (bytes == 0 || ticks == 0) {
    return 0;
  }
  return static_cast<uint64_t>(static_cast<double>(bytes) / TicksToSeconds(ticks));
}

double BytesPerSecondToMiB(uint64_t bytes_per_sec) {
  return static_cast<double>(bytes_per_sec) / kBytesPerMiB;
}

uint64_t PeakSubmitWindowTicks() {
  return static_cast<uint64_t>(kPeakSubmitWindowUs * kTscGhz * 1000.0);
}

}  // namespace

WalZrwa* g_wal_zrwa = nullptr;

void WalFreeRing::init(uint32_t cap) {
  capacity = cap;
  entries.resize(cap);
  reset();
}

void WalFreeRing::reset() {
  write_pos.store(0, std::memory_order_relaxed);
  read_pos.store(0, std::memory_order_relaxed);
}

void WalFreeRing::push(const WalRingEntry& entry) {
  uint32_t wp = write_pos.load(std::memory_order_relaxed);
  entries[wp] = entry;
  write_pos.store((wp + 1) % capacity, std::memory_order_release);
}

bool WalFreeRing::pop(WalRingEntry& entry_out) {
  while (true) {
    uint32_t rp = read_pos.load(std::memory_order_acquire);
    uint32_t wp = write_pos.load(std::memory_order_acquire);
    if (rp == wp) return false;  // empty
    entry_out = entries[rp];
    if (read_pos.compare_exchange_weak(rp, (rp + 1) % capacity,
            std::memory_order_acq_rel, std::memory_order_acquire))
      return true;
  }
}

WalZrwa::WalZrwa() {}

WalZrwa::~WalZrwa() {
  shutdown();
}

bool WalZrwa::readZrwaParams() {
  const auto* zns_ns_data = spdk_nvme_zns_ns_get_data(g_stInfo->ns);
  if (!zns_ns_data) {
    fprintf(stderr, "Failed to get ZNS namespace data\n");
    return false;
  }

  // the SPDK struct does not define these fields, read them from raw bytes
  const auto* raw = reinterpret_cast<const uint8_t*>(zns_ns_data);
  uint16_t ozcs_raw = *reinterpret_cast<const uint16_t*>(raw + 2);
  zrwa_supported_ = (ozcs_raw >> 1) & 1;

  if (!zrwa_supported_) {
    fprintf(stderr, "ZRWA not supported by device\n");
    return false;
  }

  zrwafg_    = *reinterpret_cast<const uint16_t*>(raw + 48);
  zrwa_size_ = *reinterpret_cast<const uint16_t*>(raw + 50);
  uint8_t zrwacap = *(raw + 52);

  if (!(zrwacap & 1)) {
    fprintf(stderr, "Device does not support explicit ZRWA flush\n");
  }

  return true;
}

bool WalZrwa::allocateSlots() {
  page_slots_.resize(zrwa_size_);
  dma_pages_.resize(zrwa_size_);
  ready_flags_.reset(new std::atomic<uint8_t>[zrwa_size_]);

  for (uint32_t i = 0; i < zrwa_size_; i++) {
    dma_pages_[i] = (uint8_t*)spdk_zmalloc(WAL_PAGE_SIZE, WAL_PAGE_SIZE, nullptr,
                                             SPDK_ENV_SOCKET_ID_ANY,
                                             SPDK_MALLOC_DMA);
    if (!dma_pages_[i]) {
      fprintf(stderr, "Failed to allocate DMA page %u\n", i);
      return false;
    }
    page_slots_[i].dma_idx = i;
    page_slots_[i].buf_state.store(WalPageSlot::BUF_FREE, std::memory_order_relaxed);
    page_slots_[i].seal_state.store(WalPageSlot::SealState::NONE, std::memory_order_relaxed);
    ready_flags_[i].store(0, std::memory_order_relaxed);
  }

  uint32_t ring_capacity = zrwa_size_ + 16;
  normal_ring_.init(ring_capacity);
  if (prio_window_ > 0) {
    prio_ring_.init(ring_capacity);
  }

  return true;
}

void WalZrwa::initZoneState() {
  zone_generation_.fetch_add(1, std::memory_order_acq_rel);

  zone_capacity_pages_ = active_zone_->max_capacity_ >> blk_sft_;
  zone_slba_ = active_zone_->start_ >> blk_sft_;
  uint64_t host_page = (active_zone_->wp_ - active_zone_->start_) / WAL_PAGE_SIZE;

  // on a ZRWA-opened zone the device WP advances only at ZRWAFG boundaries,
  // so an unaligned tail needs an explicit flush before start_page is set
  uint32_t fg = zrwafg_ ? zrwafg_ : 8;
  uint64_t start_page = host_page;
  uint64_t gap = host_page % fg;
  if (gap > 0) {
    uint64_t aligned = ((host_page + fg - 1) / fg) * fg;
    if (aligned > zone_capacity_pages_)
      aligned = zone_capacity_pages_;

    uint64_t flush_lba = zone_slba_ + aligned - 1;
    struct spdk_nvme_cmd cmd = {};
    cmd.opc = 0x79;  // ZONE_MGMT_SEND
    cmd.nsid = spdk_nvme_ns_get_id(g_stInfo->ns);
    cmd.cdw10 = (uint32_t)(flush_lba & 0xFFFFFFFF);
    cmd.cdw11 = (uint32_t)(flush_lba >> 32);
    cmd.cdw13 = 0x11;  // ZSA=ZRWA_FLUSH

    cb_type cb_flag;
    cb_flag.done = false;
    cb_flag.fail = false;
    int rc = spdk_nvme_ctrlr_cmd_io_raw(g_stInfo->ctrlr,
        g_stInfo->qpair[get_qpair_idx()], &cmd, nullptr, 0,
        sync_completion, &cb_flag);
    if (rc == 0) {
      while (!cb_flag.done)
        spdk_nvme_qpair_process_completions(
            g_stInfo->qpair[get_qpair_idx()], 0);
    }

    if (rc != 0 || cb_flag.fail) {
      fprintf(stderr,
              "ZRWA flush align failed, slba=0x%lx host_page=%lu aligned=%lu\n",
              zone_slba_, host_page, aligned);
    }

    start_page = aligned;

    // sync host zone state with the device
    uint64_t committed_bytes = start_page * WAL_PAGE_SIZE;
    active_zone_->wp_ = active_zone_->start_ + committed_bytes;
    active_zone_->capacity_ = active_zone_->max_capacity_ - committed_bytes;
  }

  wp_page_.store(start_page, std::memory_order_relaxed);
  tail_.store(start_page, std::memory_order_relaxed);

  // reset rings before publishing fresh slot identities
  normal_ring_.reset();
  if (prio_window_ > 0) {
    prio_ring_.reset();
  }

  if (io_thread_) {
    auto& words = io_thread_->getCompletedWords();
    for (uint32_t i = 0; i < io_thread_->getCompletedNumWords(); i++) {
      words[i].store(0, std::memory_order_relaxed);
    }
    io_thread_->resetFlushPendingWp();
  }

  uint64_t remaining_pages = zone_capacity_pages_ - start_page;
  uint32_t initial_slots = zrwa_size_;
  if (initial_slots > remaining_pages)
    initial_slots = static_cast<uint32_t>(remaining_pages);

  for (uint32_t i = 0; i < initial_slots; i++) {
    uint32_t slot_idx = (start_page + i) % zrwa_size_;
    auto& slot = page_slots_[slot_idx];
    slot.logical_page.store(start_page + i, std::memory_order_relaxed);
    slot.free_offset = PAGE_HEADER_SIZE;
    slot.page_crc = 0;
    slot.device_bytes_counted = false;
    slot.buf_state.store(WalPageSlot::BUF_FREE, std::memory_order_release);
    slot.seal_state.store(WalPageSlot::SealState::NONE, std::memory_order_relaxed);
    memset(dma_pages_[slot_idx], 0, WAL_PAGE_SIZE);
    ready_flags_[slot_idx].store(0, std::memory_order_relaxed);
    pushToRing(slot_idx);
  }

  tail_.store(start_page + initial_slots, std::memory_order_release);
  pending_writes_.store(0, std::memory_order_relaxed);
  zone_transitioning_.store(false, std::memory_order_release);
}

bool WalZrwa::openZoneZrwa(Zone* zone, spdk_nvme_qpair* qpair) {
  if (zone == nullptr) {
    fprintf(stderr, "ZRWA open requested with null zone\n");
    return false;
  }
  if (qpair == nullptr) {
    fprintf(stderr, "ZRWA open requested with null qpair, slba=0x%lx\n",
            zone->start_ >> blk_sft_);
    return false;
  }

  uint64_t slba = zone->start_ >> blk_sft_;

  struct spdk_nvme_cmd cmd = {};
  cmd.opc = 0x79;  // ZONE_MGMT_SEND
  cmd.nsid = spdk_nvme_ns_get_id(g_stInfo->ns);
  *(uint64_t*)&cmd.cdw10 = slba;
  cmd.cdw13 = 0x3 | (1 << 9);  // ZSA=Open(0x3) | ZRWAA=1 (bit 9)

  cb_type cb_flag;
  cb_flag.done = false;
  cb_flag.fail = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = qpair;
  cb_flag.slba = slba;
  cb_flag.nlb = 0;
  memcpy(cb_flag.func_name, "ZRWAOpen", 16);
#endif

  int rc = spdk_nvme_ctrlr_cmd_io_raw(g_stInfo->ctrlr, qpair, &cmd,
                                       nullptr, 0, sync_completion, &cb_flag);
  if (rc != 0) {
    fprintf(stderr, "ZRWA open command submit failed, rc=%d\n", rc);
    return false;
  }

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(qpair, 0);
  }

  if (cb_flag.fail) {
    fprintf(stderr, "ZRWA open command failed\n");
    return false;
  }

  return true;
}

bool WalZrwa::init(ZonedBlockDevice* zbd) {
  zbd_ = zbd;
  blk_sft_ = zbd->GetBlockShift();

  if (!readZrwaParams()) return false;
  prio_window_ = waltz_prio_window;

  if (!allocateSlots()) return false;

  io_thread_ = new WalZrwaIoThread();
  if (!io_thread_->init(this)) {
    fprintf(stderr, "ZRWA WAL IO thread init failed\n");
    return false;
  }

  active_zone_ = zbd_->RetrieveWalZone();
  if (!active_zone_) {
    fprintf(stderr, "Failed to retrieve initial WAL zone\n");
    return false;
  }

  initZoneState();
  io_thread_->start();

  return true;
}

void WalZrwa::printStats(const char* phase) {
  uint64_t wr = stats.nvme_write_count.load(std::memory_order_relaxed);
  uint64_t wr_lat = stats.nvme_write_lat_sum.load(std::memory_order_relaxed);
  uint64_t fl = stats.flush_count.load(std::memory_order_relaxed);
  uint64_t pay = stats.payload_bytes.load(std::memory_order_relaxed);
  uint64_t dev = stats.device_bytes.load(std::memory_order_relaxed);
  uint64_t submit = stats.submit_bytes.load(std::memory_order_relaxed);
  uint64_t ap = stats.append_count.load(std::memory_order_relaxed);
  uint64_t ap_lat = stats.append_lat_sum.load(std::memory_order_relaxed);
  uint64_t pg_used = stats.page_used_bytes_sum.load(std::memory_order_relaxed);
  uint64_t submit_first = stats.submit_first_tsc.load(std::memory_order_relaxed);
  uint64_t submit_last = stats.submit_last_tsc.load(std::memory_order_relaxed);
  uint64_t submit_window_start =
      stats.submit_window_start_tsc.load(std::memory_order_relaxed);
  uint64_t submit_window_bytes =
      stats.submit_window_bytes.load(std::memory_order_relaxed);
  uint64_t submit_peak_bps = stats.submit_peak_bps.load(std::memory_order_relaxed);

  if (submit_window_start != 0 && submit_last > submit_window_start &&
      submit_window_bytes > 0) {
    uint64_t current_window_bps =
        ComputeBytesPerSecond(submit_window_bytes, submit_last - submit_window_start);
    if (current_window_bps > submit_peak_bps) {
      submit_peak_bps = current_window_bps;
    }
  }

  uint64_t avg_submit_bps = 0;
  if (submit_first != 0 && submit_last > submit_first) {
    avg_submit_bps =
        ComputeBytesPerSecond(submit, submit_last - submit_first);
  }

  if (phase != nullptr) {
    fprintf(stderr, "\n===== ZRWA WAL Stats (%s) =====\n", phase);
  } else {
    fprintf(stderr, "\n===== ZRWA WAL Stats =====\n");
  }
  fprintf(stderr, "appendRecord calls:    %lu\n", ap);
  fprintf(stderr, "appendRecord avg lat:  %.1f ns\n",
          ap > 0 ? (double)ap_lat / ap / kTscGhz : 0.0);
  fprintf(stderr, "NVMe write count:      %lu\n", wr);
  fprintf(stderr, "NVMe write avg lat:    %.1f ns  (%.1f us)\n",
          wr > 0 ? (double)wr_lat / wr / kTscGhz : 0.0,
          wr > 0 ? (double)wr_lat / wr / kTscGhz / 1000.0 : 0.0);
  fprintf(stderr, "ZRWA flush count:      %lu\n", fl);
  fprintf(stderr, "Payload bytes:         %lu  (%.1f MB)\n", pay, pay / 1048576.0);
  fprintf(stderr, "Device bytes:          %lu  (%.1f MB)\n", dev, dev / 1048576.0);
  fprintf(stderr, "Submit bytes:          %lu  (%.1f MB)\n", submit,
          submit / 1048576.0);
  fprintf(stderr, "Avg submit throughput: %.1f MiB/s\n",
          BytesPerSecondToMiB(avg_submit_bps));
  fprintf(stderr, "Peak submit throughput: %.1f MiB/s  (%lu us bucket)\n",
          BytesPerSecondToMiB(submit_peak_bps), kPeakSubmitWindowUs);
  fprintf(stderr, "Write amplification:   %.2fx\n",
          pay > 0 ? (double)dev / pay : 0.0);
  fprintf(stderr, "Avg page utilization:  %.0f / %u bytes (%.1f%%)\n",
          wr > 0 ? (double)pg_used / wr : 0.0, WAL_PAGE_SIZE,
          wr > 0 ? (double)pg_used / wr / WAL_PAGE_SIZE * 100.0 : 0.0);
  fprintf(stderr, "Writes per append:     %.2f\n",
          ap > 0 ? (double)wr / ap : 0.0);
  uint64_t sn = stats.sync_notify_count.load(std::memory_order_relaxed);
  fprintf(stderr, "Sync notifications:    %lu\n", sn);
  fprintf(stderr, "==========================\n\n");
}

void WalZrwa::resetStats() {
  // only safe between phases, with no write in flight
  stats.nvme_write_count.store(0, std::memory_order_relaxed);
  stats.nvme_write_lat_sum.store(0, std::memory_order_relaxed);
  stats.flush_count.store(0, std::memory_order_relaxed);
  stats.payload_bytes.store(0, std::memory_order_relaxed);
  stats.device_bytes.store(0, std::memory_order_relaxed);
  stats.submit_bytes.store(0, std::memory_order_relaxed);
  stats.submit_first_tsc.store(0, std::memory_order_relaxed);
  stats.submit_last_tsc.store(0, std::memory_order_relaxed);
  stats.submit_window_start_tsc.store(0, std::memory_order_relaxed);
  stats.submit_window_bytes.store(0, std::memory_order_relaxed);
  stats.submit_peak_bps.store(0, std::memory_order_relaxed);
  stats.append_count.store(0, std::memory_order_relaxed);
  stats.append_lat_sum.store(0, std::memory_order_relaxed);
  stats.page_used_bytes_sum.store(0, std::memory_order_relaxed);
  stats.sync_notify_count.store(0, std::memory_order_relaxed);

  // per-slot device_bytes_counted is kept, so a page stays charged to the
  // phase that first wrote it
  phase_reset_used_.store(true, std::memory_order_relaxed);
}

void WalZrwa::recordDevicePageSubmit(uint64_t submit_tsc) {
  stats.submit_bytes.fetch_add(WAL_PAGE_SIZE, std::memory_order_relaxed);

  if (stats.submit_first_tsc.load(std::memory_order_relaxed) == 0) {
    stats.submit_first_tsc.store(submit_tsc, std::memory_order_relaxed);
  }
  stats.submit_last_tsc.store(submit_tsc, std::memory_order_relaxed);

  uint64_t window_start =
      stats.submit_window_start_tsc.load(std::memory_order_relaxed);
  uint64_t window_bytes =
      stats.submit_window_bytes.load(std::memory_order_relaxed);

  if (window_start == 0) {
    stats.submit_window_start_tsc.store(submit_tsc, std::memory_order_relaxed);
    stats.submit_window_bytes.store(WAL_PAGE_SIZE, std::memory_order_relaxed);
    return;
  }

  uint64_t updated_window_bytes = window_bytes + WAL_PAGE_SIZE;
  uint64_t elapsed_tsc = submit_tsc - window_start;

  if (elapsed_tsc >= PeakSubmitWindowTicks()) {
    uint64_t window_bps =
        ComputeBytesPerSecond(updated_window_bytes, elapsed_tsc);
    uint64_t peak_bps = stats.submit_peak_bps.load(std::memory_order_relaxed);
    if (window_bps > peak_bps) {
      stats.submit_peak_bps.store(window_bps, std::memory_order_relaxed);
    }
    stats.submit_window_start_tsc.store(submit_tsc, std::memory_order_relaxed);
    stats.submit_window_bytes.store(0, std::memory_order_relaxed);
    return;
  }

  stats.submit_window_bytes.store(updated_window_bytes,
                                  std::memory_order_relaxed);
}

void WalZrwa::simulateCrash() {
  // simulate SIGKILL: skip retire submit, drain and stats
  bool expected = false;
  if (!shutdown_called_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return;
  }

  // force NONE so the drain loop cannot submit more retire I/O
  zone_full_phase_ = ZoneFullPhase::NONE;

  if (io_thread_) {
    io_thread_->shutdown();  // stop() + qpair free
    delete io_thread_;
    io_thread_ = nullptr;
  }

  for (auto* p : dma_pages_) {
    if (p) spdk_free(p);
  }
  dma_pages_.clear();

  fprintf(stderr, "ZRWA WAL crash simulation done\n");
}

void WalZrwa::shutdown() {
  bool expected = false;
  if (!shutdown_called_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return;
  }
  if (io_thread_) {
    io_thread_->shutdown();
    delete io_thread_;
    io_thread_ = nullptr;
  }

  // per-phase blocks were already printed, skip the exit summary
  if (!phase_reset_used_.load(std::memory_order_relaxed) &&
      (stats.append_count.load(std::memory_order_relaxed) > 0 ||
       stats.nvme_write_count.load(std::memory_order_relaxed) > 0 ||
       stats.submit_bytes.load(std::memory_order_relaxed) > 0 ||
       stats.flush_count.load(std::memory_order_relaxed) > 0)) {
    printStats();
  }

  for (auto* p : dma_pages_) {
    if (p) spdk_free(p);
  }
  dma_pages_.clear();
}

bool WalZrwa::acquireSlot(uint32_t& slot_idx, uint16_t& avail) {
  WalRingEntry ring_entry;

  while (true) {
    if (zone_transitioning_.load(std::memory_order_acquire)) {
      // Zone is transitioning, spin
      continue;
    }

    bool popped = false;
    if (prio_window_ > 0) {
      popped = prio_ring_.pop(ring_entry);
    }
    if (!popped) {
      popped = normal_ring_.pop(ring_entry);
    }
    if (!popped) {
      // Ring empty, spin
      continue;
    }

    slot_idx = ring_entry.slot_idx;
    auto& slot = page_slots_[slot_idx];
    uint32_t current_zone_generation =
        zone_generation_.load(std::memory_order_acquire);
    uint64_t current_logical_page =
        slot.logical_page.load(std::memory_order_acquire);

    if (ring_entry.zone_generation != current_zone_generation ||
        ring_entry.expected_logical_page != current_logical_page) {
      continue;
    }

    uint8_t expected = WalPageSlot::BUF_FREE;
    if (!slot.buf_state.compare_exchange_strong(expected,
            WalPageSlot::BUF_INFLIGHT,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      // Slot not free (race), retry
      continue;
    }

    current_zone_generation = zone_generation_.load(std::memory_order_acquire);
    current_logical_page = slot.logical_page.load(std::memory_order_acquire);
    if (ring_entry.zone_generation != current_zone_generation ||
        ring_entry.expected_logical_page != current_logical_page ||
        zone_transitioning_.load(std::memory_order_acquire)) {
      slot.buf_state.store(WalPageSlot::BUF_FREE, std::memory_order_release);
      continue;
    }

    avail = WAL_PAGE_SIZE - slot.free_offset;
    return true;
  }
}

void WalZrwa::pushToRing(uint32_t slot_idx) {
  auto& slot = page_slots_[slot_idx];
  WalRingEntry entry;
  entry.slot_idx = slot_idx;
  entry.expected_logical_page = slot.logical_page.load(std::memory_order_relaxed);
  entry.zone_generation = zone_generation_.load(std::memory_order_acquire);

  if (prio_window_ == 0 || io_thread_ == nullptr) {
    normal_ring_.push(entry);
    return;
  }

  uint64_t frontier = io_thread_->getFlushPendingWp();
  uint64_t prio_limit = frontier + static_cast<uint64_t>(prio_window_);
  if (entry.expected_logical_page < prio_limit) {
    prio_ring_.push(entry);
  } else {
    normal_ring_.push(entry);
  }
}

void WalZrwa::sealAndRelease(uint32_t slot_idx) {
  auto& slot = page_slots_[slot_idx];
  slot.trySetPending();
  // the page is already on the device, no write needed
  slot.buf_state.store(WalPageSlot::BUF_SEALED, std::memory_order_release);
}

void WalZrwa::writeAndRelease(uint32_t slot_idx) {
  auto& slot = page_slots_[slot_idx];

  // seal the page if it cannot hold even a minimal record
  if (WAL_PAGE_SIZE - slot.free_offset < LOG_HEADER_SIZE + 1) {
    slot.trySetPending();
  }

  slot.buf_state.store(WalPageSlot::BUF_READY, std::memory_order_release);
  ready_flags_[slot_idx].store(1, std::memory_order_release);
  pending_writes_.fetch_add(1, std::memory_order_relaxed);
}

IOStatus WalZrwa::appendRecord(const char* payload, size_t payload_size,
                                ZrwaAppendResult* result) {
  uint64_t t0 = __builtin_ia32_rdtsc();
  const char* ptr = payload;
  size_t left = payload_size;
  bool begin = true;
  bool first_page = true;

  do {
    uint32_t slot_idx;
    uint16_t avail;

    if (!acquireSlot(slot_idx, avail)) {
      return IOStatus::IOError("ZRWA: slot acquisition failed");
    }

    // the record does not fit, seal this page and retry on a fresh one,
    // the reader expects every record complete within a single page
    if (avail < left + LOG_HEADER_SIZE) {
      if (page_slots_[slot_idx].free_offset > 0) {
        sealAndRelease(slot_idx);
        continue;
      }
      fprintf(stderr,
              "WAL record size %zu exceeds page capacity %u\n",
              left, WAL_PAGE_SIZE - PAGE_HEADER_SIZE - LOG_HEADER_SIZE);
      abort();
    }

    size_t payload_avail = avail - LOG_HEADER_SIZE;
    size_t fragment_length = (left < payload_avail) ? left : payload_avail;

    log::RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = log::kFullType;
    } else if (begin) {
      type = log::kFirstType;
    } else if (end) {
      type = log::kLastType;
    } else {
      type = log::kMiddleType;
    }

    auto& slot = page_slots_[slot_idx];
    uint8_t* page = dma_pages_[slot.dma_idx];
    uint16_t offset = slot.free_offset;

    // extent tracking starts at the first acquired slot
    if (first_page && result) {
      uint64_t lp = slot.logical_page.load(std::memory_order_acquire);
      result->start_pos = (zone_slba_ + lp) << blk_sft_;
      result->zone = active_zone_;
      result->zone_generation = zone_generation_.load(std::memory_order_acquire);
      first_page = false;
    }

    // record header: len(2), type(1)
    page[offset + 0] = static_cast<uint8_t>(fragment_length & 0xff);
    page[offset + 1] = static_cast<uint8_t>(fragment_length >> 8);
    page[offset + 2] = static_cast<uint8_t>(type);

    memcpy(page + offset + LOG_HEADER_SIZE, ptr, fragment_length);

    // trailing free space is excluded, the page header is counted once per page
    AddWalPayloadBytes(LOG_HEADER_SIZE + fragment_length);

    // update the running page CRC in the page header
    uint16_t record_size = LOG_HEADER_SIZE + fragment_length;
    slot.page_crc = crc32c::Extend(slot.page_crc,
        reinterpret_cast<const char*>(page + offset), record_size);
    EncodeFixed32(reinterpret_cast<char*>(page), crc32c::Mask(slot.page_crc));

    slot.free_offset = offset + record_size;

    // end_pos comes from the last slot written
    if (result) {
      uint64_t lp = slot.logical_page.load(std::memory_order_acquire);
      result->end_pos = (zone_slba_ + lp + 1) << blk_sft_;
    }

    // done is set by the NVMe write completion
    std::atomic<bool> done{false};
    io_thread_->getWriteCtxPool()[slot_idx].done_flag = &done;

    writeAndRelease(slot_idx);

    while (!done.load(std::memory_order_acquire)) { _mm_pause(); }

    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (left > 0);

  uint64_t t1 = __builtin_ia32_rdtsc();
  stats.append_count.fetch_add(1, std::memory_order_relaxed);
  stats.append_lat_sum.fetch_add(t1 - t0, std::memory_order_relaxed);
  stats.payload_bytes.fetch_add(payload_size, std::memory_order_relaxed);

  return IOStatus::OK();
}

bool WalZrwa::openZoneZrwaSync(Zone* zone) {
  if (zone == nullptr) {
    fprintf(stderr, "ZRWA open sync requested with null zone\n");
    return false;
  }
  return openZoneZrwa(zone, g_stInfo->qpair[get_qpair_idx()]);
}

WalZrwaIoThread::~WalZrwaIoThread() {
  shutdown();
}

bool WalZrwaIoThread::init(WalZrwa* zrwa) {
  zrwa_ = zrwa;
  replacement_zone_ = nullptr;
  retire_phase_ = RetirePhase::IDLE;

  struct spdk_nvme_io_qpair_opts opts;
  spdk_nvme_ctrlr_get_default_io_qpair_opts(g_stInfo->ctrlr, &opts, sizeof(opts));

  write_qpair_ = spdk_nvme_ctrlr_alloc_io_qpair(g_stInfo->ctrlr, &opts, sizeof(opts));
  if (!write_qpair_) {
    fprintf(stderr, "Failed to allocate WAL write qpair\n");
    return false;
  }

  // mgmt qpair for ZRWA flush and zone management
  mgmt_qpair_ = spdk_nvme_ctrlr_alloc_io_qpair(g_stInfo->ctrlr, &opts, sizeof(opts));
  if (!mgmt_qpair_) {
    fprintf(stderr, "Failed to allocate WAL mgmt qpair\n");
    return false;
  }

  uint32_t zrwa_size = zrwa->getZrwaSize();
  write_ctx_pool_.resize(zrwa_size);
  completed_num_words_ = (zrwa_size * 2 + 63) / 64;
  completed_words_.reset(new std::atomic<uint64_t>[completed_num_words_]);
  for (uint32_t i = 0; i < completed_num_words_; i++)
    completed_words_[i].store(0, std::memory_order_relaxed);

  return true;
}

void WalZrwaIoThread::start() {
  if (running_.load()) return;
  running_.store(true, std::memory_order_release);
  thread_ = std::thread([this] { ioLoop(); });
}

void WalZrwaIoThread::stop() {
  if (!running_.load()) return;
  running_.store(false, std::memory_order_release);
  if (thread_.joinable()) thread_.join();
}

void WalZrwaIoThread::shutdown() {
  stop();
  replacement_zone_ = nullptr;
  retire_phase_ = RetirePhase::IDLE;
  if (mgmt_qpair_) {
    int c;
    do { c = spdk_nvme_qpair_process_completions(mgmt_qpair_, 0); } while (c > 0);
    spdk_nvme_ctrlr_free_io_qpair(mgmt_qpair_);
    mgmt_qpair_ = nullptr;
  }
  if (write_qpair_) {
    int c;
    do { c = spdk_nvme_qpair_process_completions(write_qpair_, 0); } while (c > 0);
    spdk_nvme_ctrlr_free_io_qpair(write_qpair_);
    write_qpair_ = nullptr;
  }
}

void WalZrwaIoThread::ioLoop() {
  while (running_.load(std::memory_order_relaxed)) {
    spdk_nvme_qpair_process_completions(write_qpair_, 0);
    spdk_nvme_qpair_process_completions(mgmt_qpair_, 0);

    scanAndSubmitWrites();
    processSealedSlots();

    // skip during DRAINING, the retire flush covers the zone end
    if (zrwa_->getZoneFullPhase() == ZoneFullPhase::NONE)
      checkAdvance();

    processZoneFullPhase();
    refillSlots();
  }

  // drain remaining completions
  uint32_t idle = 0;
  while (idle < 8) {
    int c = spdk_nvme_qpair_process_completions(write_qpair_, 0);
    c += spdk_nvme_qpair_process_completions(mgmt_qpair_, 0);
    int w = scanAndSubmitWrites();
    processSealedSlots();
    checkAdvance();
    processZoneFullPhase();
    if (c == 0 && w == 0) idle++;
    else idle = 0;
  }
}

// mark no-write sealed pages as completed
void WalZrwaIoThread::processSealedSlots() {
  uint32_t zrwa_size = zrwa_->getZrwaSize();
  for (uint32_t i = 0; i < zrwa_size; i++) {
    auto& slot = zrwa_->getSlot(i);
    if (slot.buf_state.load(std::memory_order_acquire) != WalPageSlot::BUF_SEALED)
      continue;
    if (slot.seal_state.load(std::memory_order_acquire) != WalPageSlot::SealState::PENDING)
      continue;
    if (slot.tryEnterInProgress()) {
      markCompleted(slot.logical_page.load(std::memory_order_relaxed));
      slot.finishSeal();
    }
  }
}

int WalZrwaIoThread::scanAndSubmitWrites() {
  int submitted = 0;
  uint32_t zrwa_size = zrwa_->getZrwaSize();

  for (uint32_t slot_idx = 0; slot_idx < zrwa_size; slot_idx++) {
    if (zrwa_->getReadyFlag(slot_idx).load(std::memory_order_acquire) == 0)
      continue;

    auto& slot = zrwa_->getSlot(slot_idx);

    if (slot.buf_state.load(std::memory_order_acquire) != WalPageSlot::BUF_READY) {
      zrwa_->getReadyFlag(slot_idx).store(0, std::memory_order_relaxed);
      continue;
    }

    auto& wctx = write_ctx_pool_[slot_idx];
    wctx.slot_idx = slot_idx;
    wctx.logical_page = slot.logical_page.load(std::memory_order_relaxed);
    wctx.io_thread = this;
    wctx.submit_tsc = __builtin_ia32_rdtsc();

    uint64_t lba = zrwa_->getZoneSlba() + wctx.logical_page;

    slot.buf_state.store(WalPageSlot::BUF_INFLIGHT, std::memory_order_release);
    zrwa_->getReadyFlag(slot_idx).store(0, std::memory_order_relaxed);

    // one page is one LBA at 4K block size
    int rc = spdk_nvme_ns_cmd_write(g_stInfo->ns, write_qpair_,
                                     zrwa_->getDmaPage(slot.dma_idx),
                                     lba, 1,
                                     writeCompleteCb, &wctx, 0);
    if (rc != 0) {
      // queue full, restore READY for retry
      slot.buf_state.store(WalPageSlot::BUF_READY, std::memory_order_release);
      zrwa_->getReadyFlag(slot_idx).store(1, std::memory_order_release);
      break;
    }
    zrwa_->recordDevicePageSubmit(wctx.submit_tsc);
    submitted++;
  }
  return submitted;
}

void WalZrwaIoThread::writeCompleteCb(void* arg, const struct spdk_nvme_cpl* cpl) {
  auto* ctx = static_cast<WriteCtx*>(arg);
  auto* done = ctx->done_flag;
  ctx->done_flag = nullptr;
  auto* self = ctx->io_thread;
  auto* zrwa = self->zrwa_;

  if (spdk_nvme_cpl_is_error(cpl)) {
    fprintf(stderr, "WAL page write failed, slot=%u page=%lu sct=%u sc=%u\n",
            ctx->slot_idx, ctx->logical_page, cpl->status.sct, cpl->status.sc);
    if (done) done->store(true, std::memory_order_release);
    abort();
  }

  uint64_t lat = __builtin_ia32_rdtsc() - ctx->submit_tsc;
  zrwa->stats.nvme_write_count.fetch_add(1, std::memory_order_relaxed);
  zrwa->stats.nvme_write_lat_sum.fetch_add(lat, std::memory_order_relaxed);

  auto& slot = zrwa->getSlot(ctx->slot_idx);
  if (!slot.device_bytes_counted) {
    zrwa->stats.device_bytes.fetch_add(WalZrwa::WAL_PAGE_SIZE,
                                       std::memory_order_relaxed);
    AddWalDeviceBytes(WalZrwa::WAL_PAGE_SIZE);
    AddTotalDeviceBytes(WalZrwa::WAL_PAGE_SIZE);
    // the page CRC header is counted once per logical page
    AddWalPayloadBytes(WalZrwa::PAGE_HEADER_SIZE);
    slot.device_bytes_counted = true;
  }
  zrwa->stats.page_used_bytes_sum.fetch_add(slot.free_offset, std::memory_order_relaxed);
  zrwa->getPendingWrites().fetch_sub(1, std::memory_order_release);

  // a seal may have been requested while the write was in flight
  if (slot.seal_state.load(std::memory_order_acquire) ==
      WalPageSlot::SealState::PENDING) {
    slot.buf_state.store(WalPageSlot::BUF_SEALED, std::memory_order_release);
    if (slot.tryEnterInProgress()) {
      self->markCompleted(ctx->logical_page);
      slot.finishSeal();
    }
    if (done) done->store(true, std::memory_order_release);
    return;
  }

  slot.buf_state.store(WalPageSlot::BUF_FREE, std::memory_order_release);

  if (!zrwa->getTransitioning().load(std::memory_order_acquire)) {
    zrwa->pushToRing(ctx->slot_idx);
  }

  if (done) {
    zrwa->stats.sync_notify_count.fetch_add(1, std::memory_order_relaxed);
    done->store(true, std::memory_order_release);
  }
}

void WalZrwaIoThread::markCompleted(uint64_t logical_page) {
  uint64_t wp = zrwa_->getWpPage().load(std::memory_order_acquire);
  if (logical_page < wp) return;  // stale

  uint32_t zrwa_size = zrwa_->getZrwaSize();
  uint64_t offset = logical_page - wp;
  if (offset >= zrwa_size) return;  // out of range

  // the bitmap is circular over 2x zrwa_size bits
  uint32_t bit_pos = static_cast<uint32_t>(logical_page % (zrwa_size * 2));
  completed_words_[bit_pos / 64].fetch_or(
      1ULL << (bit_pos % 64), std::memory_order_release);
}

// scan the bitmap for consecutive completed pages and flush them
void WalZrwaIoThread::checkAdvance() {
  uint64_t wp = zrwa_->getWpPage().load(std::memory_order_acquire);
  uint32_t zrwa_size = zrwa_->getZrwaSize();

  uint64_t count = 0;
  uint32_t bm_size = zrwa_size * 2;
  uint32_t start_bit = static_cast<uint32_t>(wp % bm_size);
  uint32_t remaining = zrwa_size;
  uint32_t pos = start_bit;

  while (remaining > 0) {
    uint32_t word_idx = pos / 64;
    uint32_t bit_in_word = pos % 64;
    uint64_t word = completed_words_[word_idx].load(std::memory_order_acquire);

    word >>= bit_in_word;
    uint32_t bits_left_in_word = 64 - bit_in_word;
    if (bits_left_in_word > remaining) {
      word &= (1ULL << remaining) - 1;
      bits_left_in_word = remaining;
    }

    if (bits_left_in_word < 64 && word == (1ULL << bits_left_in_word) - 1) {
      count += bits_left_in_word;
      remaining -= bits_left_in_word;
      pos = (pos + bits_left_in_word) % bm_size;
    } else if (bits_left_in_word == 64 && word == UINT64_MAX) {
      count += 64;
      remaining -= 64;
      pos = (pos + 64) % bm_size;
    } else {
      count += (word == 0) ? 0 : __builtin_ctzll(~word);
      break;
    }
  }

  if (count == 0) return;

  // round down to the flush minimum
  if (zrwa_exp_flush) {
    uint32_t min_flush = zrwa_->getZrwaFg();
    if (min_flush < WalZrwa::MIN_FLUSH_PAGES)
      min_flush = WalZrwa::MIN_FLUSH_PAGES;
    count = (count / min_flush) * min_flush;
    if (count == 0) return;
  }

  // already covered by a pending flush
  if (wp + count <= flush_pending_wp_) return;

  if (!zrwa_exp_flush) {
    uint64_t target_wp = wp + count;
    flush_pending_wp_ = target_wp;

    uint32_t bm_size = zrwa_size * 2;
    uint32_t start_bit = static_cast<uint32_t>(wp % bm_size);
    for (uint64_t i = 0; i < count; i++) {
      uint32_t bit_pos = (start_bit + static_cast<uint32_t>(i)) % bm_size;
      completed_words_[bit_pos / 64].fetch_and(
          ~(1ULL << (bit_pos % 64)), std::memory_order_relaxed);
    }

    zrwa_->getWpPage().store(target_wp, std::memory_order_release);

    Zone* zone = zrwa_->getActiveZone();
    if (zone) {
      uint64_t committed_bytes = target_wp * WalZrwa::WAL_PAGE_SIZE;
      zone->wp_       = zone->start_ + committed_bytes;
      zone->capacity_ = zone->max_capacity_ - committed_bytes;
    }
    return;
  }

  sendFlush(count);
}

void WalZrwaIoThread::sendFlush(uint64_t count) {
  uint64_t wp = zrwa_->getWpPage().load(std::memory_order_relaxed);
  uint64_t flush_lba = zrwa_->getZoneSlba() + wp + count - 1;

  uint64_t target_wp = wp + count;
  flush_pending_wp_ = target_wp;

  zrwa_->stats.flush_count.fetch_add(1, std::memory_order_relaxed);

  auto* ctx = new FlushCtx{this, count, target_wp, zrwa_->getZoneSlba()};

  struct spdk_nvme_cmd cmd = {};
  cmd.opc = 0x79;  // ZONE_MGMT_SEND
  cmd.nsid = spdk_nvme_ns_get_id(g_stInfo->ns);
  cmd.cdw10 = (uint32_t)(flush_lba & 0xFFFFFFFF);
  cmd.cdw11 = (uint32_t)(flush_lba >> 32);
  cmd.cdw13 = 0x11;  // ZSA=ZRWA_FLUSH

  int rc = spdk_nvme_ctrlr_cmd_io_raw(g_stInfo->ctrlr, mgmt_qpair_, &cmd,
                                       nullptr, 0, flushCompleteCb, ctx);
  if (rc != 0) {
    // queue full, roll back
    flush_pending_wp_ = wp;
    delete ctx;
  }
}

bool WalZrwaIoThread::submitRetireClose(Zone* zone) {
  if (zone == nullptr) {
    fprintf(stderr, "Zone retire requested with null zone\n");
    return false;
  }

  uint64_t slba = zrwa_->getZoneSlba();
  uint64_t cap_pages = zrwa_->getZoneCapacityPages();

  if (!cap_pages) {
    completeRetireFinish(zone);
    return true;
  }

  auto* ctx = new RetireCtx{this, zone, slba};

  auto* qpair = zrwa_exp_flush ? mgmt_qpair_ : write_qpair_;
  int rc = spdk_nvme_zns_finish_zone(g_stInfo->ns, qpair,
                                     slba, false,
                                     retireFinishCompleteCb, ctx);
  if (rc != 0) {
    fprintf(stderr, "Zone finish submit failed, rc=%d slba=0x%lx\n", rc, slba);
    delete ctx;
    abort();
  }

  retire_phase_ = RetirePhase::FINISH_SUBMITTED;
  wal_finish_complete_.store(false, std::memory_order_release);
  return true;
}

void WalZrwaIoThread::completeRetireFinish(Zone* zone) {
  uint64_t zone_sz = zrwa_->getZbd()->GetZoneSize();
  zone->capacity_ = 0;
  zone->wp_ = zone->start_ + zone_sz;
  zrwa_->getZbd()->NotifyIOZoneFull(zone);
  zrwa_->getZbd()->NotifyIOZoneClosed(zone);
  bool released = zone->Release();
  assert(released);
  retire_phase_ = RetirePhase::COMPLETE;
  wal_finish_complete_.store(true, std::memory_order_release);
}

void WalZrwaIoThread::flushCompleteCb(void* arg, const struct spdk_nvme_cpl* cpl) {
  auto* ctx = static_cast<FlushCtx*>(arg);
  auto* self = ctx->self;
  auto* zrwa = self->zrwa_;

  // stale flush from a previous zone
  if (ctx->zone_slba != zrwa->getZoneSlba()) {
    delete ctx;
    return;
  }

  if (spdk_nvme_cpl_is_error(cpl)) {
    uint64_t cur_wp = zrwa->getWpPage().load(std::memory_order_relaxed);
    bool stale = (ctx->target_wp <= cur_wp);
    fprintf(stderr,
            "ZRWA flush %s, sct=%u sc=%u, "
            "target_wp=%lu cur_wp=%lu count=%lu zone_slba=0x%lx cap=%lu\n",
            stale ? "overlapped an earlier flush" : "failed",
            cpl->status.sct, cpl->status.sc,
            ctx->target_wp, cur_wp, ctx->count,
            zrwa->getZoneSlba(), zrwa->getZoneCapacityPages());
    delete ctx;
    return;
  }

  uint64_t target_wp = ctx->target_wp;
  uint64_t cur_wp = zrwa->getWpPage().load(std::memory_order_relaxed);

  if (target_wp > cur_wp) {
    uint64_t flushed = target_wp - cur_wp;
    uint32_t zrwa_size = zrwa->getZrwaSize();
    uint32_t bm_size = zrwa_size * 2;
    uint32_t start_bit = static_cast<uint32_t>(cur_wp % bm_size);

    for (uint64_t i = 0; i < flushed; i++) {
      uint32_t bit_pos = (start_bit + static_cast<uint32_t>(i)) % bm_size;
      self->completed_words_[bit_pos / 64].fetch_and(
          ~(1ULL << (bit_pos % 64)), std::memory_order_relaxed);
    }

    zrwa->getWpPage().store(target_wp, std::memory_order_release);

    // sync host zone state with the device
    Zone* zone = zrwa->getActiveZone();
    if (zone) {
      uint64_t committed_bytes = target_wp * WalZrwa::WAL_PAGE_SIZE;
      zone->wp_       = zone->start_ + committed_bytes;
      zone->capacity_ = zone->max_capacity_ - committed_bytes;
    }
  }

  delete ctx;
}

void WalZrwaIoThread::retireFinishCompleteCb(void* arg,
                                             const struct spdk_nvme_cpl* cpl) {
  auto* ctx = static_cast<RetireCtx*>(arg);
  auto* self = ctx->self;

  if (spdk_nvme_cpl_is_error(cpl)) {
    fprintf(stderr, "Zone finish failed, slba=0x%lx sct=%u sc=%u\n",
            ctx->zone_slba, cpl->status.sct, cpl->status.sc);
    delete ctx;
    abort();
  }

  self->completeRetireFinish(ctx->zone);
  delete ctx;
}

// the IO thread retires the old zone itself and switches only once the
// retired zone is finished and a replacement zone is ready
void WalZrwaIoThread::processZoneFullPhase() {
  ZoneFullPhase phase = zrwa_->getZoneFullPhase();

  switch (phase) {
    case ZoneFullPhase::NONE:
      return;

    case ZoneFullPhase::DRAINING: {
      if (zrwa_->getPendingWrites().load(std::memory_order_acquire) > 0)
        return;

      // an INFLIGHT slot can also sit in the gap between CAS claim and
      // pending_writes_ increment
      uint32_t zrwa_size = zrwa_->getZrwaSize();
      for (uint32_t i = 0; i < zrwa_size; i++) {
        if (zrwa_->getSlot(i).buf_state.load(std::memory_order_acquire) ==
            WalPageSlot::BUF_INFLIGHT)
          return;  // writer still mid-claim, retry next iteration
      }

      // seal residual partial pages and pending seals
      for (uint32_t i = 0; i < zrwa_size; i++) {
        auto& slot = zrwa_->getSlot(i);
        uint8_t state = slot.buf_state.load(std::memory_order_acquire);
        if (state != WalPageSlot::BUF_FREE) continue;

        auto seal = slot.seal_state.load(std::memory_order_acquire);
        uint64_t lp = slot.logical_page.load(std::memory_order_relaxed);

        if (seal == WalPageSlot::SealState::PENDING) {
          markCompleted(lp);
          slot.finishSeal();
          slot.buf_state.store(WalPageSlot::BUF_SEALED, std::memory_order_release);
        } else if (seal == WalPageSlot::SealState::IN_PROGRESS) {
          markCompleted(lp);
          slot.finishSeal();
          slot.buf_state.store(WalPageSlot::BUF_SEALED, std::memory_order_release);
        } else if (slot.free_offset > 0) {
          // partial page with data, no seal requested
          markCompleted(lp);
          slot.buf_state.store(WalPageSlot::BUF_SEALED, std::memory_order_release);
        }
      }

      // a flush overlapping the retire can confuse the firmware ZRWA state
      {
        uint64_t wp = zrwa_->getWpPage().load(std::memory_order_acquire);
        if (flush_pending_wp_ > wp) return;
      }

      if (retire_phase_ == RetirePhase::IDLE) {
        Zone* old_zone = zrwa_->getActiveZone();
        if (old_zone == nullptr) {
          fprintf(stderr, "Draining with a null active zone\n");
          return;
        }
        if (!submitRetireClose(old_zone)) {
          return;
        }
        zrwa_->setActiveZone(nullptr);
      }

      if (replacement_zone_ == nullptr) {
        replacement_zone_ = zrwa_->getZbd()->TryRetrieveWalZone();
      }

      if (retire_phase_ != RetirePhase::COMPLETE || replacement_zone_ == nullptr) {
        return;
      }

      zrwa_->setActiveZone(replacement_zone_);
      replacement_zone_ = nullptr;
      retire_phase_ = RetirePhase::IDLE;
      zrwa_->initZoneState();

      // initZoneState has already cleared zone_transitioning_
      zrwa_->setZoneFullPhase(ZoneFullPhase::NONE);
      return;
    }
  }
}

// advance the tail and reassign sealed or free slots to new pages
void WalZrwaIoThread::refillSlots() {
  if (zrwa_->getZoneFullPhase() != ZoneFullPhase::NONE) return;

  uint64_t tail_val = zrwa_->getTail().load(std::memory_order_relaxed);
  uint64_t wp = zrwa_->getWpPage().load(std::memory_order_relaxed);
  uint32_t zrwa_size = zrwa_->getZrwaSize();

  // cached capacity, max_capacity_ can change under concurrent zone management
  uint64_t zone_capacity_pages = zrwa_->getZoneCapacityPages();

  uint64_t window = zrwa_size;
  while (tail_val < wp + window && tail_val < zone_capacity_pages) {
    uint32_t slot_idx = tail_val % zrwa_size;
    auto& slot = zrwa_->getSlot(slot_idx);

    uint8_t state = slot.buf_state.load(std::memory_order_acquire);
    if (state != WalPageSlot::BUF_SEALED && state != WalPageSlot::BUF_FREE) {
      break;  // cannot refill past an in-use slot
    }
    if (slot.seal_state.load(std::memory_order_acquire) !=
        WalPageSlot::SealState::NONE) {
      break;  // seal pending, processSealedSlots handles it first
    }

    // reset the slot for a new page
    slot.logical_page.store(tail_val, std::memory_order_relaxed);
    slot.free_offset = WalZrwa::PAGE_HEADER_SIZE;
    slot.page_crc = 0;
    slot.device_bytes_counted = false;
    slot.buf_state.store(WalPageSlot::BUF_FREE, std::memory_order_release);
    slot.seal_state.store(WalPageSlot::SealState::NONE, std::memory_order_relaxed);
    memset(zrwa_->getDmaPage(slot.dma_idx), 0, WalZrwa::WAL_PAGE_SIZE);
    zrwa_->getReadyFlag(slot_idx).store(0, std::memory_order_relaxed);
    zrwa_->pushToRing(slot_idx);

    tail_val++;
  }

  zrwa_->getTail().store(tail_val, std::memory_order_relaxed);

  // the zone is full, start draining
  if (tail_val >= zone_capacity_pages && zone_capacity_pages > 0 &&
      !zrwa_->getTransitioning().load(std::memory_order_relaxed)) {
    zrwa_->getTransitioning().store(true, std::memory_order_release);
    zrwa_->setZoneFullPhase(ZoneFullPhase::DRAINING);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
