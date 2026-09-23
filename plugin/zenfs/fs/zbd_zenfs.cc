// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"
#include "wal_zrwa.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "monitoring/waf_stats.h"
#include "snapshot.h"
#include "spdk/nvme_zns.h"

#include "port/likely.h"

extern bool waltz_mode;
extern std::string zns_pcie_addr;

zns_info *g_stInfo = nullptr;
bool g_bAllocationRunning = false;
thread_local int qpair_idx = -1;
std::atomic<int> qpair_allocator;

int get_qpair_idx() {
  if (UNLIKELY(qpair_idx == -1)) {
    qpair_idx = qpair_allocator.fetch_add(1);
  }

  return qpair_idx;
}

inline uint8_t zbd_zone_type(struct spdk_nvme_zns_zone_desc *z) { return z->zt; }
inline bool zbd_zone_swr(struct spdk_nvme_zns_zone_desc *z) { return (z->zt == SPDK_NVME_ZONE_TYPE_SEQWR); }
inline uint8_t zbd_zone_cond(struct spdk_nvme_zns_zone_desc *z) { return z->zs; }
inline bool zbd_zone_empty(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_EMPTY); }
inline bool zbd_zone_imp_open(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_IOPEN); }
inline bool zbd_zone_exp_open(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_EOPEN); }
inline bool zbd_zone_is_open(struct spdk_nvme_zns_zone_desc *z) { return (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)); }
inline bool zbd_zone_closed(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_CLOSED); }
inline bool zbd_zone_is_active(struct spdk_nvme_zns_zone_desc *z) { return (zbd_zone_is_open(z) || zbd_zone_closed(z)); }
inline bool zbd_zone_full(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_FULL); }
inline bool zbd_zone_rdonly(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_RONLY); }
inline bool zbd_zone_offline(struct spdk_nvme_zns_zone_desc *z) { return (z->zs == SPDK_NVME_ZONE_STATE_OFFLINE); }
inline uint64_t zbd_zone_start(struct spdk_nvme_zns_zone_desc *z) { return z->zslba; }
inline uint64_t zbd_zone_capacity(struct spdk_nvme_zns_zone_desc *z) { return z->zcap; }
inline uint64_t zbd_zone_wp(struct spdk_nvme_zns_zone_desc *z) { return z->wp; }

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

// [JS] SPDK completion callbacks
void append_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl) {
  cb_type *cb_param = (cb_type *)cb_arg;

  if (spdk_nvme_cpl_is_error(cpl)) {
    if (cpl->status.sc == 0xb9) { // Zone full
      cb_param->fail = true;
      cb_param->done = true;
      return;
    }

    fprintf(stderr, "ZNS error on append completion\n");

    // in error case, stop running
    while(1);
  }

  if (cb_arg != nullptr) {
    // [JS] set alba (append logical block address) from cdw1-cdw0
    cb_param->alba = (((uint64_t)cpl->cdw1) << 32) | (uint64_t)cpl->cdw0;

    // [JS] set done flag
    cb_param->done = true;
  }
}
void close_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl) {
  cb_type *cb_param = (cb_type *)cb_arg;

  if (spdk_nvme_cpl_is_error(cpl)) {
    //fprintf(stderr, "ZNS error on close completion, retry\n");
    cb_param->fail = true;
  }

  if (cb_arg != nullptr) {
    // [JS] set done flag
    cb_param->done = true;
  }
}
void sync_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl) {
  cb_type *cb_param = (cb_type *)cb_arg;

  if (spdk_nvme_cpl_is_error(cpl)) {
#if defined(CB_ARG_DEBUG)
    fprintf(stderr, "ZNS error on sync_completion: func=%s slba=0x%lx nlb=%lu qpair_idx=%u SCT=%u SC=%u\n",
            cb_param->func_name, cb_param->slba, cb_param->nlb,
            cb_param->idx, cpl->status.sct, cpl->status.sc);
#else
    fprintf(stderr, "ZNS error on sync_completion: SCT=%u SC=%u\n",
            cpl->status.sct, cpl->status.sc);
#endif
    abort();
  }

  if (cb_arg != nullptr) {
    // [JS] set done flag
    cb_param->done = true;
  }
}

std::atomic<int> async_cmd_count;

void async_completion(void * /*cb_arg*/, const struct spdk_nvme_cpl *cpl) {
  if (spdk_nvme_cpl_is_error(cpl)) {
    fprintf(stderr, "ZNS error on async completion\n");
  }
  async_cmd_count.fetch_sub(1);
}

namespace ROCKSDB_NAMESPACE {

bool zrwa_open_zone(Zone* zone, ZonedBlockDevice* zbd) {
  uint64_t slba = zone->start_ >> zbd->GetBlockShift();

  struct spdk_nvme_cmd cmd = {};
  cmd.opc = 0x79;  // ZONE_MGMT_SEND
  cmd.nsid = spdk_nvme_ns_get_id(g_stInfo->ns);
  *(uint64_t*)&cmd.cdw10 = slba;
  cmd.cdw13 = 0x3 | (1 << 9);  // ZSA=Open(0x3) | ZRWAA=1

  cb_type cb_flag;
  cb_flag.done = false;
  cb_flag.fail = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = slba;
  cb_flag.nlb = 0;
  memcpy(cb_flag.func_name, "ZRWAOpen", 16);
#endif

  int rc = spdk_nvme_ctrlr_cmd_io_raw(g_stInfo->ctrlr,
      g_stInfo->qpair[get_qpair_idx()], &cmd, nullptr, 0,
      sync_completion, &cb_flag);
  if (rc != 0) {
    fprintf(stderr, "ZRWA open submit failed slba=0x%lx rc=%d\n", slba, rc);
    return false;
  }

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }

  if (cb_flag.fail) {
    fprintf(stderr, "ZRWA open failed slba=0x%lx\n", slba);
    return false;
  }

  zone->MarkWalZrwaOpened();
  return true;
}
uint64_t zone_size;
static void AllocationThread(void *_zbd) {
  ZonedBlockDevice *zbd = (ZonedBlockDevice *)_zbd;

  while(g_bAllocationRunning) {
    zbd->BackgroundJobForWAL();
  }
}

Zone::Zone(ZonedBlockDevice *zbd, struct spdk_nvme_zns_zone_desc *z)
    : zbd_(zbd),
      busy_(false),
      wal_zrwa_opened_(false),
      start_(zbd_zone_start(z) << zbd->GetBlockShift()),
      max_capacity_(zbd_zone_capacity(z) << zbd->GetBlockShift()),
      wp_(zbd_zone_wp(z) << zbd->GetBlockShift()) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = (zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z))) << zbd->GetBlockShift();
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

IOStatus Zone::CloseWR() {
  assert(IsBusy());

  if (WasWalZrwaOpened() && capacity_ == 0) {
    IOStatus status = Finish();
    if (!status.ok()) return status;
    zbd_->NotifyIOZoneFull(this);
    return IOStatus::OK();
  }

  IOStatus status = Close();

  if (capacity_ == 0) {
    zbd_->NotifyIOZoneFull(this);
  }

  return status;
}

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  struct spdk_nvme_zns_zone_report *report;
  uint32_t report_nbytes;
  int ret;
  cb_type cb_flag;

  assert(!IsUsed());
  assert(IsBusy());

  if (zone_size == 0) { zone_size = (uint64_t)zone_sz; }

  cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = start_ >> zbd_->GetBlockShift();
  cb_flag.nlb = 0;
  memcpy(cb_flag.func_name, "Reset", 16);
#endif
  ret = spdk_nvme_zns_reset_zone(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], start_ >> zbd_->GetBlockShift(), false, sync_completion, &cb_flag);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }

  report_nbytes = sizeof(report->descs[0]) + sizeof(*report);
  report = (struct spdk_nvme_zns_zone_report *)calloc(1, report_nbytes);

  bool report_retry = false;
  do {
    cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
    cb_flag.idx = get_qpair_idx();
    cb_flag.ns = g_stInfo->ns;
    cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
    cb_flag.slba = start_ >> zbd_->GetBlockShift();
    cb_flag.nlb = 0;
    memcpy(cb_flag.func_name, "Reset_report", 16);
#endif
    ret = spdk_nvme_zns_report_zones(
        g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], report, report_nbytes, start_ >> zbd_->GetBlockShift(), SPDK_NVME_ZRA_LIST_ALL,
        true, sync_completion, &cb_flag);

    while (!cb_flag.done) {
      spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
    }

    if (ret || (report->nr_zones != 1)) {
      report_retry = true;
    } else {
      report_retry = false;
    }
  } while (report_retry);

  struct spdk_nvme_zns_zone_desc *z = &report->descs[0];

  if (zbd_zone_offline(z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(z) << zbd_->GetBlockShift();

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;
  ClearWalZrwaOpened();

  free(report);

  return IOStatus::OK();
}

IOStatus Zone::ResetAsync() {
  size_t zone_sz = zbd_->GetZoneSize();
  struct spdk_nvme_zns_zone_report *report;
  uint32_t report_nbytes;
  int ret;
  cb_type cb_flag;

  assert(!IsUsed());
  assert(IsBusy());

  if (zone_size == 0) { zone_size = (uint64_t)zone_sz; }

  cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = start_ >> zbd_->GetBlockShift();
  cb_flag.nlb = 0;
  memcpy(cb_flag.func_name, "Reset", 16);
#endif
  ret = spdk_nvme_zns_reset_zone(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], start_ >> zbd_->GetBlockShift(), false, async_completion, &cb_flag);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  async_cmd_count.fetch_add(1);
  ClearWalZrwaOpened();

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int ret;
  cb_type cb_flag;

  assert(IsBusy());

  cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = start_ >> zbd_->GetBlockShift();
  cb_flag.nlb = 0;
  memcpy(cb_flag.func_name, "Finish", 16);
#endif
  ret = spdk_nvme_zns_finish_zone(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], start_ >> zbd_->GetBlockShift(), false, sync_completion, &cb_flag);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }

  capacity_ = 0;
  wp_ = start_ + zone_sz;
  ClearWalZrwaOpened();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int ret;
  cb_type cb_flag;

  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    do{
    cb_flag.done = false;
    cb_flag.fail = false;
#if defined(CB_ARG_DEBUG)
    cb_flag.idx = get_qpair_idx();
    cb_flag.ns = g_stInfo->ns;
    cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
    cb_flag.slba = start_ >> zbd_->GetBlockShift();
    cb_flag.nlb = 0;
    memcpy(cb_flag.func_name, "Close", 16);
#endif
    ret = spdk_nvme_zns_close_zone(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], start_ >> zbd_->GetBlockShift(), false, close_completion, &cb_flag);
    if (ret) return IOStatus::IOError("Zone close failed\n");

    while (!cb_flag.done) {
      spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
    }
    } while (cb_flag.fail);
  }

  return IOStatus::OK();
}


IOStatus Zone::Append(char *data, uint32_t size, uint64_t *alba, bool is_wal) {
  char *ptr = data;
  uint32_t left = size;
  int ret;
  cb_type cb_flag;

  assert((size % zbd_->GetBlockSize()) == 0);

  cb_flag.fail = false;
  cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = start_ >> zbd_->GetBlockShift();
  cb_flag.nlb = size >> zbd_->GetBlockShift();
  memcpy(cb_flag.func_name, "ZoneAppend", 16);
#endif
  ret = spdk_nvme_zns_zone_append(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], ptr, start_ >> zbd_->GetBlockShift(), size >> zbd_->GetBlockShift(), append_completion, &cb_flag, 0);
  if (ret < 0) return IOStatus::IOError("Write failed");

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }

  if (alba) {
    *alba = cb_flag.alba;
  }

  if (cb_flag.fail) {
    return IOStatus::NoSpace();
  }

  AddTotalDeviceBytes(size);
  if (is_wal) {
    AddWalDeviceBytes(size);
  }
  return IOStatus::OK();
}

IOStatus Zone::Write(char *data, uint32_t size, bool is_wal) {
  char *ptr = data;
  uint32_t left = size;
  int ret;
  cb_type cb_flag;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  size_t mdts = zbd_->GetMDTS();
  while(left) {
  size = left;
  if (size > mdts) {
    // if nlb is more than mdts, cut off
    size = mdts;
  }

  cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
  cb_flag.idx = get_qpair_idx();
  cb_flag.ns = g_stInfo->ns;
  cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
  cb_flag.slba = wp_ >> zbd_->GetBlockShift();
  cb_flag.nlb = size >> zbd_->GetBlockShift();
  memcpy(cb_flag.func_name, "ZoneWrite", 16);
#endif
  ret = spdk_nvme_ns_cmd_write(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], ptr, wp_ >> zbd_->GetBlockShift(), size >> zbd_->GetBlockShift(), sync_completion, &cb_flag, 0);
  if (ret < 0) return IOStatus::IOError("Write failed");

  ptr += size;
  capacity_ -= size;
  wp_ += size;
  left -= size;

  while (!cb_flag.done) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }

  AddTotalDeviceBytes(size);
  if (is_wal) {
    AddWalDeviceBytes(size);
  }
  }

  return IOStatus::OK();
}

inline IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : filename_("/dev/" + bdevname),
      logger_(logger),
      metrics_(metrics),
      allocation_thread_(nullptr) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
  wal_queue_cnt_.store(0);
  wal_finish_queue_cnt_.store(0);
  active_io_zones_.store(0);
  open_io_zones_.store(0);
}

int ZonedBlockDevice::GetWalZoneReserveTarget() const {
  return (waltz_wal_mode == "zrwa") ? 1 : wal_zone_reserve_count_;
}

int ZonedBlockDevice::WalZoneDeficit() const {
  int target = GetWalZoneReserveTarget();
  int current = wal_queue_cnt_.load(std::memory_order_relaxed);
  return std::max(0, target - current);
}

bool ZonedBlockDevice::CanAllocateMoreOpenZones(bool for_wal) const {
  long limit = static_cast<long>(max_nr_open_io_zones_);
  if (!for_wal) limit -= WalZoneDeficit();
  return open_io_zones_.load() < limit;
}

bool ZonedBlockDevice::CanAllocateMoreActiveZones(bool for_wal) const {
  long limit = static_cast<long>(max_nr_active_io_zones_);
  if (!for_wal) limit -= WalZoneDeficit();
  return active_io_zones_.load() < limit;
}

void ZonedBlockDevice::AccountZoneActiveChange(int delta) {
  active_io_zones_.fetch_add(delta, std::memory_order_relaxed);
}

void ZonedBlockDevice::AccountZoneOpenChange(int delta) {
  open_io_zones_.fetch_add(delta, std::memory_order_relaxed);
}

std::string ZonedBlockDevice::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus ZonedBlockDevice::CheckScheduler() {
  return IOStatus::OK();
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0, 5);  // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    fprintf(stderr, "Failed to Open NVMe Scheduler %s\n", __func__);
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument(
        "Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

Status ZonedBlockDevice::Format(){
  for (const auto z : meta_zones) {
    if (z->Acquire()) {
      if (!z->ResetAsync().ok()) Warn(logger_, "Failed reseting zone");
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
    }
  }
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->ResetAsync().ok()) Warn(logger_, "Failed reseting zone");
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;

      while (async_cmd_count.load() >= 256) {
        spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
      }
    }
  }
  while (async_cmd_count.load()) {
    spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
  }
  return Status::OK();
}

// [JS] SPDK-related struct & definition
static int get_zns_nsid() {
  if (zns_pcie_addr.find("d8:00") != std::string::npos ||
      zns_pcie_addr.find("af:00") != std::string::npos ||
      zns_pcie_addr.find("86:00") != std::string::npos ||
      zns_pcie_addr.find("5e:00") != std::string::npos ||
      zns_pcie_addr.find("18:00") != std::string::npos)
    return 2;  // All WD 2600 devices use ns2
  return 1;
}
#define NVME_ZNS_NAMESPACE (get_zns_nsid())

// [JS] SPDK probe callback
static bool zns_probe(void * /*cb_ctx*/,
    const struct spdk_nvme_transport_id * /*trid*/,
    struct spdk_nvme_ctrlr_opts * /*opts*/) {
  return true;
}
// [JS] SPDK attach callback
static void zns_attach(void * /*cb_ctx*/,
    const struct spdk_nvme_transport_id * /*trid*/,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) {
  struct spdk_nvme_io_qpair_opts qp_opts;
  spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &qp_opts, sizeof(qp_opts));

  g_stInfo->ctrlr = ctrlr;
  g_stInfo->ns    = spdk_nvme_ctrlr_get_ns(ctrlr, NVME_ZNS_NAMESPACE);

  if (g_stInfo->ns == nullptr) {
    fprintf(stderr, "Namespace %d not found\n", NVME_ZNS_NAMESPACE);
    return;
  }

  uint8_t csi = spdk_nvme_ns_get_csi(g_stInfo->ns);

  g_stInfo->qpair = (struct spdk_nvme_qpair **)malloc(sizeof(struct spdk_nvme_qpair *) * NUM_WORKER_THREAD);
  for (int i = 0; i < NUM_WORKER_THREAD; i ++) {
    g_stInfo->qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &qp_opts, sizeof(qp_opts));
  }
  qpair_allocator.store(0);

  g_stInfo->maxqd = opts->io_queue_size;

  if (csi == SPDK_NVME_CSI_ZNS) {
    g_stInfo->zns = spdk_nvme_zns_ns_get_data(g_stInfo->ns);
    if (spdk_nvme_ctrlr_get_flags(ctrlr) & SPDK_NVME_CTRLR_ZONE_APPEND_SUPPORTED) {
      g_stInfo->append_support = true;
    }
  } else {
    fprintf(stderr, "Namespace %d is not ZNS (CSI=%d)\n", NVME_ZNS_NAMESPACE, csi);
  }
  g_stInfo->valid = true;
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  struct spdk_nvme_zns_zone_report *report;
  uint32_t report_nbytes;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  int ret;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  // [JS] check if this is the first trial to open zoned block device
  if (g_stInfo == nullptr) {
    // [JS] SPDK probe & attach
    g_stInfo = new zns_info;
    struct spdk_env_opts opts;
    struct spdk_nvme_transport_id trid_pcie;

    spdk_env_opts_init(&opts);
    opts.name = "ZNS";
    opts.mem_size = 0;      // use all available hugepages
    opts.mem_channel = 1;
    spdk_env_init(&opts);

    memset(&trid_pcie, 0, sizeof(trid_pcie));
    spdk_nvme_trid_populate_transport(&trid_pcie, SPDK_NVME_TRANSPORT_PCIE);
    snprintf(trid_pcie.subnqn, sizeof(trid_pcie.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);
    {
      std::string traddr_str = "trtype:PCIe traddr:" + zns_pcie_addr;
      if (spdk_nvme_transport_id_parse(&trid_pcie, traddr_str.c_str()) != 0) {
        fprintf(stderr, "Error parsing transport address: %s\n", traddr_str.c_str());
      }
      spdk_nvme_transport_id_populate_trstring(&trid_pcie,
          spdk_nvme_transport_id_trtype_str(trid_pcie.trtype));
    }
    spdk_nvme_probe(&trid_pcie, nullptr, zns_probe, zns_attach, nullptr);

    spdk_unaffinitize_thread();

    if (g_stInfo->zns == nullptr) {
      return IOStatus::NotSupported("Not a host managed block device");
    }

    if (g_stInfo->append_support == false) {
      return IOStatus::NotSupported("Append not supported");
    }

    wal_update_extent_buffer_ = (char *)spdk_zmalloc(4096, sysconf(_SC_PAGESIZE), 0, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  }

  {
    const struct spdk_nvme_ns_data *nsdata = spdk_nvme_ns_get_data(g_stInfo->ns);
    uint8_t flbas_idx = nsdata->flbas.format;  // active LBA format index
    blk_sft_ = nsdata->lbaf[flbas_idx].lbads;
  }
  block_sz_ = (1 << blk_sft_);
  blk_msk_  = ~((((uint64_t)1) << blk_sft_) - 1);
  zone_sz_  = spdk_nvme_zns_ns_get_zone_size(g_stInfo->ns);
  nr_zones_ = spdk_nvme_zns_ns_get_num_zones(g_stInfo->ns);

  if (nr_zones_ < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  /* We need one open zone for meta data writes, the rest can be used for files
   */
  uint32_t max_nr_active_zones_from_dev =
    spdk_nvme_zns_ns_get_max_active_zones(g_stInfo->ns);
  uint32_t max_nr_open_zones_from_dev =
    spdk_nvme_zns_ns_get_max_open_zones(g_stInfo->ns);

  uint32_t reserved_zones = 1;  // reserve 1 slot for metadata zone

  if (max_nr_active_zones_from_dev == 0)
    max_nr_active_io_zones_ = nr_zones_;
  else
    max_nr_active_io_zones_ =
        (max_nr_active_zones_from_dev > reserved_zones)
            ? max_nr_active_zones_from_dev - reserved_zones
            : 0;

  if (max_nr_open_zones_from_dev == 0)
    max_nr_open_io_zones_ = nr_zones_;
  else
    max_nr_open_io_zones_ =
        (max_nr_open_zones_from_dev > reserved_zones)
            ? max_nr_open_zones_from_dev - reserved_zones
            : 0;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       nr_zones_, max_nr_active_zones_from_dev, max_nr_open_zones_from_dev);

  report_nbytes = sizeof(report->descs[0]) * nr_zones_ + sizeof(*report);
  report = (struct spdk_nvme_zns_zone_report *)calloc(1, report_nbytes);

  max_data_xfer_size_ = spdk_nvme_ns_get_max_io_xfer_size(g_stInfo->ns);
  uint32_t report_xferbytes = max_data_xfer_size_;
  struct spdk_nvme_zns_zone_report *report_xferbuf = (struct spdk_nvme_zns_zone_report *) calloc(1, report_xferbytes);
  uint32_t handled_zones = 0;
  uint64_t slba = 0;
  size_t zdes = 0;
  uint32_t zds, zrs;
  uint64_t zone_size_lba = spdk_nvme_zns_ns_get_zone_size_sectors(g_stInfo->ns);
  int rc = 0;

  while (handled_zones < nr_zones_) {
    memset(report_xferbuf, 0, report_xferbytes);
    cb_type cb_flag;

    cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
    cb_flag.idx = get_qpair_idx();
    cb_flag.ns = g_stInfo->ns;
    cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
    cb_flag.slba = slba;
    cb_flag.nlb = 0;
    memcpy(cb_flag.func_name, "Open_report", 16);
#endif
    if (zdes) {
      rc = spdk_nvme_zns_ext_report_zones(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], report_xferbuf, report_xferbytes,
            slba, SPDK_NVME_ZRA_LIST_ALL, true, sync_completion, &cb_flag);
    } else {
      rc = spdk_nvme_zns_report_zones(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], report_xferbuf, report_xferbytes,
            slba, SPDK_NVME_ZRA_LIST_ALL, true, sync_completion, &cb_flag);
    }

    if (rc) {
      fprintf(stderr, "Report zones failed\n");
      exit(1);
    }

    while (!cb_flag.done) {
      spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
    }

    for (i = 0; i < report_xferbuf->nr_zones; i++) {
      memcpy((void *)&(report->descs[handled_zones]), (void *)&(report_xferbuf->descs[i]), sizeof(struct spdk_nvme_zns_zone_desc));
      slba += zone_size_lba;
      handled_zones++;
    }
  }

  report->nr_zones = handled_zones;

  m = 0;
  i = 0;

  while (m < ZENFS_META_ZONES && i < nr_zones_) {
    struct spdk_nvme_zns_zone_desc *z = &report->descs[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == SPDK_NVME_ZONE_TYPE_SEQWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  for (; i < nr_zones_; i++) {
    struct spdk_nvme_zns_zone_desc *z = &report->descs[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == SPDK_NVME_ZONE_TYPE_SEQWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        // closing a ZRWA-opened zone drops pages still inside the ZRWA
        // window, recovery must read them before the zone is closed
        bool skip_auto_close = (waltz_wal_mode == "zrwa") &&
                               (zbd_zone_wp(z) > zbd_zone_start(z));
        if ((zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) && !readonly &&
            !skip_auto_close) {
          newZone->Close();
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) return status;
      }
    }
  }

  active_io_zones_.store(0, std::memory_order_relaxed);
  open_io_zones_.store(0, std::memory_order_relaxed);
  for (const auto z : io_zones) {
    if (!z->IsEmpty() && !z->IsFull()) {
      AccountZoneActiveChange(1);
    }
  }

  free(report_xferbuf);
  free(report);
  start_time_ = time(NULL);

  // nsid 1 opens ZRWA on every zone, nsid 2 only on WAL zones
  zrwa_open_all_zones_ = (waltz_wal_mode == "zrwa" && get_zns_nsid() == 1);

  return IOStatus::OK();
}

void ZonedBlockDevice::NotifyIOZoneFull(Zone *zone) {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  AccountZoneActiveChange(-1);
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyIOZoneClosed(Zone *zone) {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  AccountZoneOpenChange(-1);
  zone_resources_.notify_one();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  static time_t last_log_time = 0;
  time_t now = time(NULL);
  if (now - last_log_time < 10) return;  // at most once per 10 seconds
  last_log_time = now;

  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx.lock();

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_total(#), "
       "open_total(#)] %ld %lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(std::memory_order_relaxed),
       open_io_zones_.load(std::memory_order_relaxed));

  io_zones_mtx.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  // Shutdown ZRWA before destroying zones
  if (rocksdb::g_wal_zrwa) {
    delete rocksdb::g_wal_zrwa;
    rocksdb::g_wal_zrwa = nullptr;
  }

  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  if (waltz_mode) {
    g_bAllocationRunning = false;
    if (allocation_thread_) {
      allocation_thread_->join();
      delete allocation_thread_;
      allocation_thread_ = nullptr;
    }
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

Status ZonedBlockDevice::ResetUnusedIOZones() {
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      // keep zones that still carry unflushed bytes for the recovery path
      if (waltz_wal_mode == "zrwa" && !z->IsUsed() && !z->IsEmpty() &&
          z->wp_ > z->start_) {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
        continue;
      }
      if (!z->IsUsed() && !z->IsEmpty()) {
        if (!z->IsFull()) {
          AccountZoneActiveChange(-1);
        }
        if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
      }
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
    }
  }
  return Status::OK();
}

IOStatus ZonedBlockDevice::AllocateZoneFromPool(
    Env::WriteLifeTimeHint file_lifetime, Zone **out_zone, bool empty_only,
    bool for_wal) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;
  bool ok = false;
  (void)ok;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_IO_ALLOC_NON_WAL_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  *out_zone = nullptr;

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  io_zones_mtx.lock();

  /* Make sure we are below the zone open limit. io_zones_mtx is released
   * around the wait, the allocation thread needs it to free a zone. */
  while (!CanAllocateMoreOpenZones(for_wal)) {
    io_zones_mtx.unlock();
    {
      std::unique_lock<std::mutex> lk(zone_resources_mtx_);
      zone_resources_.wait(lk, [this, for_wal] {
        return CanAllocateMoreOpenZones(for_wal);
      });
    }
    io_zones_mtx.lock();
  }

  /* Reset any unused zones and finish used zones under capacity treshold*/
  for (const auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty() || (z->IsFull() && z->IsUsed())) {
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
      continue;
    }

    if (!z->IsUsed()) {
      // wp > start means unflushed WAL bytes with no metadata yet, recovery
      // claims the zone later and resetting it here would wipe the WAL
      if (waltz_wal_mode == "zrwa" && z->wp_ > z->start_) {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
        continue;
      }
      if (!z->IsFull()) {
        AccountZoneActiveChange(-1);
      }
      s = z->Reset();
      if (!s.ok()) {
        Debug(logger_, "Failed resetting zone !");
        return s;
      }

      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
      continue;
    }

    if ((z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
      /* If there is less than finish_threshold_% remaining capacity in a
       * non-open-zone, finish the zone */
      s = z->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
        return s;
      }
      AccountZoneActiveChange(-1);
    }

    if (!z->IsFull()) {
      if (finish_victim == nullptr) {
        finish_victim = z;
      } else if (finish_victim->capacity_ > z->capacity_) {
        IOStatus status = finish_victim->CheckRelease();
        if (!status.ok()) return status;
        finish_victim = z;
      } else {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
      }
    } else {
      IOStatus status = z->CheckRelease();
      if (!status.ok()) return status;
    }
  }

  // Holding finish_victim if != nullptr

  /* Try to fill an already open zone(with the best life time diff) */
  if (empty_only) goto find_empty;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if ((z->used_capacity_ > 0) && !z->IsFull()) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {
            IOStatus status = allocated_zone->CheckRelease();
            if (!status.ok()) return status;
          }
          allocated_zone = z;
          best_diff = diff;
        } else {
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
        }
      } else {
        IOStatus status = z->CheckRelease();
        if (!status.ok()) return status;
      }
    }
  }

  // Holding finish_victim if != nullptr
  // Holding allocated_zone if != nullptr

find_empty:
  /* If we did not find a good match, allocate an empty one */
  if (empty_only || best_diff >= LIFETIME_DIFF_NOT_GOOD) {
    /* If we at the active io zone limit, finish an open zone(if available) with
     * least capacity left */
    if (!CanAllocateMoreActiveZones(for_wal) && finish_victim != nullptr) {
      s = finish_victim->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
        return s;
      }
      AccountZoneActiveChange(-1);
    }

    if (CanAllocateMoreActiveZones(for_wal)) {
      for (const auto z : io_zones) {
        if (z->Acquire()) {
          if (z->IsEmpty() && z->capacity_ > 0) {
            z->lifetime_ = file_lifetime;
            if (allocated_zone != nullptr) {
              IOStatus status = allocated_zone->CheckRelease();
              if (!status.ok()) return status;
            }
            allocated_zone = z;
            AccountZoneActiveChange(1);
            new_zone = 1;
            break;
          } else {
            IOStatus status = z->CheckRelease();
            if (!status.ok()) return status;
          }
        }
      }
    }
  }

  if (finish_victim != nullptr) {
    IOStatus status = finish_victim->CheckRelease();
    if (!status.ok()) return status;
    finish_victim = nullptr;
  }

  if (allocated_zone) {
    ok = allocated_zone->IsBusy();
    assert(ok);
    // unified mode only, WAL-only mode opens ZRWA in BackgroundJobForWAL
    if (waltz_wal_mode == "zrwa" && zrwa_open_all_zones_) {
      if (!zrwa_open_zone(allocated_zone, this)) {
        if (new_zone) AccountZoneActiveChange(-1);
        IOStatus status = allocated_zone->CheckRelease();
        if (!status.ok()) return status;
        allocated_zone = nullptr;
        *out_zone = nullptr;
        io_zones_mtx.unlock();
        return IOStatus::OK();
      }
    }
    AccountZoneOpenChange(1);
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d "
          "file lt: %d\n",
          new_zone, allocated_zone->start_,
          allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  }

  io_zones_mtx.unlock();
  LogZoneStats();

  *out_zone = allocated_zone;

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES, active_io_zones_);

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint file_lifetime,
                                        Zone **out_zone, bool empty_only) {
  return AllocateZoneFromPool(file_lifetime, out_zone, empty_only, false);
}

IOStatus ZonedBlockDevice::AllocateWalZone(Zone **out_zone, bool empty_only) {
  return AllocateZoneFromPool(Env::WLTH_SHORT, out_zone, empty_only, true);
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }

uint32_t ZonedBlockDevice::GetBlockShift() { return blk_sft_; }

uint64_t ZonedBlockDevice::GetBlockMask() { return blk_msk_; }

uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto &zone : io_zones) snapshot.emplace_back(*zone);
}

void ZonedBlockDevice::BackgroundJobForWAL() {
  if (wal_finish_queue_cnt_.load()) {
    Zone *f_zone;
    {
      std::lock_guard<std::mutex> lk(wal_queue_mtx_);
      f_zone = wal_finish_zone_queue_.front();
      wal_finish_zone_queue_.pop();
      wal_finish_queue_cnt_.fetch_sub(1);
    }
    if (f_zone->WasWalZrwaOpened() || f_zone->capacity_) {
      f_zone->Finish();
    }
    NotifyIOZoneFull(f_zone);
    f_zone->Close();
    NotifyIOZoneClosed(f_zone);
    f_zone->Release();
    /* return after finish, the freed active slot must become visible to
     * compaction before a new WAL zone takes it */
    return;
  }

  if (wal_queue_cnt_.load() < GetWalZoneReserveTarget()) {
    // defer refill until the retiring zone Finish completes, WAL keeps at
    // most two device-active zones
    if (rocksdb::g_wal_zrwa != nullptr &&
        !rocksdb::g_wal_zrwa->getIoThread()->isWalFinishComplete())
      return;

    Zone *n_zone;
    bool need_empty = !zrwa_open_all_zones_ && waltz_wal_mode == "zrwa";
    IOStatus s = AllocateWalZone(&n_zone, need_empty);
    if (s.ok()) {
      if (n_zone == nullptr) {
        return;
      }
      // WAL-only mode opens the zone from the WalZrwa IO thread
      if (need_empty && rocksdb::g_wal_zrwa != nullptr) {
        if (!rocksdb::g_wal_zrwa->openZoneZrwaSync(n_zone)) {
          fprintf(stderr, "ZRWA open failed slba=0x%lx\n",
                  n_zone->start_ >> blk_sft_);
          NotifyIOZoneFull(n_zone);
          NotifyIOZoneClosed(n_zone);
          n_zone->Release();
          return;
        }
      }
      {
        std::lock_guard<std::mutex> lk(wal_queue_mtx_);
        wal_zone_queue_.push(n_zone);
        wal_queue_cnt_.fetch_add(1);
      }
    }
  }
}
Zone *ZonedBlockDevice::RetrieveWalZone() {
  if (!g_bAllocationRunning) {
    g_bAllocationRunning = true;
    allocation_thread_ = new std::thread(AllocationThread, this);
  }
  Zone *zone = nullptr;
  do {
    if (wal_queue_cnt_.load()) {
      std::lock_guard<std::mutex> lk(wal_queue_mtx_);
      if (!wal_zone_queue_.empty()) {
        zone = wal_zone_queue_.front();
        wal_zone_queue_.pop();
        wal_queue_cnt_.fetch_sub(1);
      }
    }
  } while (zone == nullptr);
  return zone;
}

Zone *ZonedBlockDevice::TryRetrieveWalZone() {
  if (!g_bAllocationRunning) {
    g_bAllocationRunning = true;
    allocation_thread_ = new std::thread(AllocationThread, this);
  }
  if (wal_queue_cnt_.load()) {
    std::lock_guard<std::mutex> lk(wal_queue_mtx_);
    if (!wal_zone_queue_.empty()) {
      Zone *zone = wal_zone_queue_.front();
      wal_zone_queue_.pop();
      wal_queue_cnt_.fetch_sub(1);
      return zone;
    }
  }
  return nullptr;
}

void ZonedBlockDevice::FinishWalZone(Zone *req_zone) {
  {
    std::lock_guard<std::mutex> lk(wal_queue_mtx_);
    wal_finish_zone_queue_.push(req_zone);
    wal_finish_queue_cnt_.fetch_add(1);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
