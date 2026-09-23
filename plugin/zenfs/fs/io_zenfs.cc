// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "io_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "util/coding.h"
#include "wal_zrwa.h"

extern bool waltz_mode;
extern bool zenfs_skip_meta_sync;

namespace ROCKSDB_NAMESPACE {

extern Env *g_env;
extern uint64_t write_logging[32][16];
extern void prt_time(int pos);
extern thread_local int logging_idx;

// time spent in ZoneFile::Recover, reported with the WAL replay breakdown
std::atomic<uint64_t> g_wal_extent_recover_ns{0};
std::atomic<uint64_t> g_wal_extent_recover_count{0};
// defined in db/db_impl/db_impl_open.cc
extern std::atomic<uint64_t> g_wal_read_dev_bytes;
extern std::atomic<uint64_t> g_wal_read_dev_ios;

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone* zone)
    : start_(start), length_(length), zone_(zone) {}

Status ZoneExtent::DecodeFrom(Slice* input) {
  if (input->size() != (sizeof(start_) + sizeof(length_)))
    return Status::Corruption("ZoneExtent", "Error: length missmatch");

  GetFixed64(input, &start_);
  GetFixed32(input, &length_);
  return Status::OK();
}

void ZoneExtent::EncodeTo(std::string* output) {
  PutFixed64(output, start_);
  PutFixed32(output, length_);
}

void ZoneExtent::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"length\":" << length_;
  json_stream << "}";
}

enum ZoneFileTag : uint32_t {
  kFileID = 1,
  kFileName = 2,
  kFileSize = 3,
  kWriteLifeTimeHint = 4,
  kExtent = 5,
  kModificationTime = 6,
  kActiveExtentStart = 7,
};

void ZoneFile::EncodeTo(std::string* output, uint32_t extent_start) {
  PutFixed32(output, kFileID);
  PutFixed64(output, file_id_);

  PutFixed32(output, kFileName);
  PutLengthPrefixedSlice(output, Slice(filename_));

  PutFixed32(output, kFileSize);
  PutFixed64(output, fileSize);

  PutFixed32(output, kWriteLifeTimeHint);
  PutFixed32(output, (uint32_t)lifetime_);

  for (uint32_t i = extent_start; i < extents_.size(); i++) {
    std::string extent_str;

    PutFixed32(output, kExtent);
    extents_[i]->EncodeTo(&extent_str);
    PutLengthPrefixedSlice(output, Slice(extent_str));
  }

  PutFixed32(output, kModificationTime);
  PutFixed64(output, (uint64_t)m_time_);

  /* Keep the start LBA of the in-flight extent so reopen can recover it */
  PutFixed32(output, kActiveExtentStart);
  PutFixed64(output, extent_start_);
}

void ZoneFile::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"id\":" << file_id_ << ",";
  json_stream << "\"filename\":\"" << filename_ << "\",";
  json_stream << "\"size\":" << fileSize << ",";
  json_stream << "\"hint\":" << lifetime_ << ",";
  json_stream << "\"extents\":[";

  bool first_element = true;
  for (ZoneExtent* extent : extents_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    extent->EncodeJson(json_stream);
  }
  json_stream << "]}";
}

Status ZoneFile::DecodeFrom(Slice* input) {
  uint32_t tag = 0;

  GetFixed32(input, &tag);
  if (tag != kFileID || !GetFixed64(input, &file_id_))
    return Status::Corruption("ZoneFile", "File ID missing");

  while (true) {
    Slice slice;
    ZoneExtent* extent;
    Status s;

    if (!GetFixed32(input, &tag)) break;

    switch (tag) {
      case kFileName:
        if (!GetLengthPrefixedSlice(input, &slice))
          return Status::Corruption("ZoneFile", "Filename missing");
        filename_ = slice.ToString();
        if (filename_.length() == 0)
          return Status::Corruption("ZoneFile", "Zero length filename");
        break;
      case kFileSize:
        if (!GetFixed64(input, &fileSize))
          return Status::Corruption("ZoneFile", "Missing file size");
        break;
      case kWriteLifeTimeHint:
        uint32_t lt;
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing life time hint");
        lifetime_ = (Env::WriteLifeTimeHint)lt;
        break;
      case kExtent:
        extent = new ZoneExtent(0, 0, nullptr);
        GetLengthPrefixedSlice(input, &slice);
        s = extent->DecodeFrom(&slice);
        if (!s.ok()) {
          delete extent;
          return s;
        }
        extent->zone_ = zbd_->GetIOZone(extent->start_);
        if (!extent->zone_)
          return Status::Corruption("ZoneFile", "Invalid zone extent");
        extent->zone_->used_capacity_ += extent->length_;
        extents_.push_back(extent);
        break;
      case kModificationTime:
        uint64_t ct;
        if (!GetFixed64(input, &ct))
          return Status::Corruption("ZoneFile", "Missing creation time");
        m_time_ = (time_t)ct;
        break;
      case kActiveExtentStart:
        uint64_t es;
        if (!GetFixed64(input, &es))
          return Status::Corruption("ZoneFile", "Active extent start");
        extent_start_ = es;
        break;
      default:
        return Status::Corruption("ZoneFile", "Unexpected tag");
    }
  }

  MetadataSynced();
  return Status::OK();
}

Status ZoneFile::MergeUpdate(std::shared_ptr<ZoneFile> update) {
  if (file_id_ != update->GetID())
    return Status::Corruption("ZoneFile update", "ID missmatch");

  Rename(update->GetFilename());
  SetFileSize(update->GetFileSize());
  SetWriteLifeTimeHint(update->GetWriteLifeTimeHint());
  SetFileModificationTime(update->GetFileModificationTime());

  std::vector<ZoneExtent*> update_extents = update->GetExtents();
  for (long unsigned int i = 0; i < update_extents.size(); i++) {
    ZoneExtent* extent = update_extents[i];
    Zone* zone = extent->zone_;
    zone->used_capacity_ += extent->length_;
    extents_.push_back(new ZoneExtent(extent->start_, extent->length_, zone));
  }

  extent_start_ = update->GetExtentStart();
  MetadataSynced();

  return Status::OK();
}

ZoneFile::ZoneFile(ZonedBlockDevice* zbd, std::string filename,
                   uint64_t file_id)
    : zbd_(zbd),
      active_zone_(NULL),
      extent_start_(NO_EXTENT),
      extent_filepos_(0),
      lifetime_(Env::WLTH_NOT_SET),
      fileSize(0),
      filename_(filename),
      file_id_(file_id),
      nr_synced_extents_(0),
      m_time_(0) {
}

std::string ZoneFile::GetFilename() { return filename_; }
void ZoneFile::Rename(std::string name) { filename_ = name; }
time_t ZoneFile::GetFileModificationTime() { return m_time_; }

uint64_t ZoneFile::GetFileSize() { return fileSize; }
void ZoneFile::SetFileSize(uint64_t sz) { fileSize = sz; }
void ZoneFile::SetFileModificationTime(time_t mt) { m_time_ = mt; }

ZoneFile::~ZoneFile() {
  if (simulated_crash_) {
    // simulate SIGKILL: leave the device state as is
    // extents_ and active_zone_ are released when the zbd closes
    return;
  }
  for (auto e = std::begin(extents_); e != std::end(extents_); ++e) {
    Zone* zone = (*e)->zone_;

    assert(zone && zone->used_capacity_ >= (*e)->length_);
    zone->used_capacity_ -= (*e)->length_;
    delete *e;
  }
  IOStatus s = CloseWR();
  if (!s.ok()) {
    zbd_->SetZoneDeferredStatus(s);
  }
}

IOStatus ZoneFile::CloseWR() {
  if (simulated_crash_) {
    // simulate SIGKILL: leave the device and the metadata as is
    // extent_start_ must stay set until the next reopen recovers it
    return IOStatus::OK();
  }
  IOStatus s = IOStatus::OK();

  /* No in-flight extent left after a clean close */
  extent_start_ = NO_EXTENT;

  s = CloseActiveZone();
  open_for_wr_ = false;

  return s;
}

IOStatus ZoneFile::CloseActiveZone() {
  IOStatus s = IOStatus::OK();
  if (active_zone_) {
    Zone* closing_zone = active_zone_;
    s = active_zone_->CloseWR();
    if (!s.ok()) {
      return s;
    }
    ReleaseActiveZone();
    zbd_->NotifyIOZoneClosed(closing_zone);
  }
  return s;
}

void ZoneFile::OpenWR() { open_for_wr_ = true; }

bool ZoneFile::IsOpenForWR() { return open_for_wr_; }

ZoneExtent* ZoneFile::GetExtent(uint64_t file_offset, uint64_t* dev_offset) {
  for (unsigned int i = 0; i < extents_.size(); i++) {
    if (file_offset < extents_[i]->length_) {
      *dev_offset = extents_[i]->start_ + file_offset;
      return extents_[i];
    } else {
      file_offset -= extents_[i]->length_;
    }
  }
  return NULL;
}

struct read_meta {
  char *buffer;
  uint64_t offset;
  uint64_t size;
  cb_type cb_flag;
};

IOStatus ZoneFile::PositionedRead(uint64_t offset, size_t n, Slice* result,
                                  char* scratch, bool direct) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_READ_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportQPS(ZENFS_READ_QPS, 1);

  // read offset ++ n bytes from file
  // result will be stored at scratch buffer and result slice

  // first, analyze the device chunks
  size_t remain_bytes;
  size_t read_bytes = 0;
  ZoneExtent* extent;
  uint64_t buf_off = 0;
  uint64_t extent_end;
  uint64_t dev_off;
  uint64_t chunk_sz;
  char *buffer;
  int ret;
  std::queue<read_meta*> meta_q;
  IOStatus s;

  // Limit read size to end of file
  if ((offset + n) > fileSize) {
    remain_bytes = fileSize - offset;
  } else {
    remain_bytes = n;
  }

  // [DU]
  int retry_cnt = 0;
  uint64_t tgt_offset = 0;

  const bool is_wal_read = filename_.size() >= 4 &&
      filename_.compare(filename_.size() - 4, 4, ".log") == 0;

  while(remain_bytes) {
    int cur_retry = 0; // [DU]
    extent = GetExtent(offset, &dev_off);
    if (!extent) {
      /* read beyond end of (synced) file data */
      break;
    }

    extent_end = extent->start_ + extent->length_;

    chunk_sz = extent_end - dev_off;

    // if remaining bytes is smaller than chunk size, reduce it
    if (remain_bytes < chunk_sz) {
      chunk_sz = remain_bytes;
    }

    read_meta *cur_meta = new read_meta;
    uint64_t read_start = dev_off & zbd_->GetBlockMask();
    uint64_t read_end = (dev_off + chunk_sz + zbd_->GetBlockSize() - 1) & zbd_->GetBlockMask();
    uint64_t read_cnt = read_end - read_start;
    if (is_wal_read) {
      g_wal_read_dev_bytes.fetch_add(read_cnt, std::memory_order_relaxed);
      g_wal_read_dev_ios.fetch_add(1, std::memory_order_relaxed);
    }
    buffer = (char *)spdk_zmalloc(read_cnt, sysconf(_SC_PAGESIZE), 0, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    // [DU] retry code
    while (buffer == nullptr)
    {
        //printf("Buffer Allocation Fail\n");
        // Try to release buffer
        while(!meta_q.empty()) {
            auto cur_meta = meta_q.front();

            while(!cur_meta->cb_flag.done);

            memcpy(scratch + tgt_offset, cur_meta->buffer + cur_meta->offset, cur_meta->size);

            tgt_offset += cur_meta->size;
            spdk_free(cur_meta->buffer);

            meta_q.pop();
            printf("Early release success %d %d \n", retry_cnt, cur_retry);
        }

        buffer = (char *)spdk_zmalloc(read_cnt, sysconf(_SC_PAGESIZE), 0, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        cur_retry += 1;
    }
    retry_cnt += cur_retry;
    // [DU] retry code end

    cur_meta->cb_flag.done = false;
#if defined(CB_ARG_DEBUG)
    cur_meta->cb_flag.idx = get_qpair_idx();
    cur_meta->cb_flag.ns = g_stInfo->ns;
    cur_meta->cb_flag.qpair = g_stInfo->qpair[get_qpair_idx()];
    cur_meta->cb_flag.slba = read_start >> zbd_->GetBlockShift();
    cur_meta->cb_flag.nlb = read_cnt >> zbd_->GetBlockShift();
    memcpy(cur_meta->cb_flag.func_name, "PositionedRead", 16);
#endif

    // issue the read request of current chunk
    // read dev_off ++ chunk_sz from device, and update it at buf_off ++ chunk_sz at scratch buffer
    ret = spdk_nvme_ns_cmd_read(g_stInfo->ns, g_stInfo->qpair[get_qpair_idx()], buffer,
            read_start >> zbd_->GetBlockShift(), read_cnt >> zbd_->GetBlockShift(),
	        sync_completion, &cur_meta->cb_flag, 0);

    if (ret < 0) {
  	  read_bytes = 0;
      s = IOStatus::IOError("Read error\n");
    }

    cur_meta->buffer = buffer;
    cur_meta->offset = dev_off & ~(zbd_->GetBlockMask());
    cur_meta->size = chunk_sz;

    meta_q.push(cur_meta);

    offset += chunk_sz;
    remain_bytes -= chunk_sz;
    buf_off += chunk_sz;
    read_bytes += chunk_sz;
  }

  //uint64_t tgt_offset = 0; // [DU] disable
  while(!meta_q.empty()) {
    auto cur_meta = meta_q.front();

    while (!cur_meta->cb_flag.done) {
      spdk_nvme_qpair_process_completions(g_stInfo->qpair[get_qpair_idx()], 0);
    }

    memcpy(scratch + tgt_offset, cur_meta->buffer + cur_meta->offset, cur_meta->size);

    tgt_offset += cur_meta->size;
    spdk_free(cur_meta->buffer);

    meta_q.pop();
  }

  *result = Slice((char*)scratch, read_bytes);
  return s;
}

void ZoneFile::PushExtent() {
  uint64_t length;

  assert(fileSize >= extent_filepos_);

  if (!active_zone_) return;

  length = fileSize - extent_filepos_;
  if (length == 0) return;

  assert(length <= (active_zone_->wp_ - extent_start_));
  extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_));

  active_zone_->used_capacity_ += length;
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = fileSize;
}

/* Assumes that data and size are block aligned */
IOStatus ZoneFile::Append(void* data, int data_size, int valid_size) {
  uint32_t left = data_size;
  uint32_t wr_size, offset = 0;
  IOStatus s = IOStatus::OK();

  prt_time(8);

  // in zrwa mode the WAL is written by WalZrwa
  if (waltz_mode == true && lifetime_ == Env::WLTH_SHORT && g_wal_zrwa != nullptr) {
    return IOStatus::OK();
  }

  // [JS] for WAL file, use another path
  if (waltz_mode == true && lifetime_ == Env::WLTH_SHORT && g_wal_zrwa == nullptr) {
    // zone append mode, buffer is already allocated from SPDK DMA area
    s = IOStatus::NoSpace();
    uint64_t alba;

    do {
      // Try zone allocation or wait for it
      if (!active_zone_) {
        if (wal_zone_mutex_.try_lock()) {
          prt_time(9);
          active_zone_ = zbd_->RetrieveWalZone();
          prt_time(10);
          extent_start_ = active_zone_->wp_;
          wal_zone_mutex_.unlock();
          PersistMetadataIfWal();
        } else {
          continue;
        }
      }

      s = active_zone_->Append((char*)data, data_size, &alba, /*is_wal=*/true);
      prt_time(11);

      if (!s.ok() || (active_zone_ && zbd_->CheckNewWalZoneNeeded(active_zone_, alba))) {
        if (wal_zone_mutex_.try_lock()) {
          prt_time(12);
          if (active_zone_) {
            UpdateWalExtent();
            zbd_->FinishWalZone(active_zone_);
          }
          active_zone_ = nullptr;
          wal_zone_mutex_.unlock();
          PersistMetadataIfWal();
        } else {
          continue;
        }
      }

    } while (!s.ok());

    // skip all the other information updates
    return s;
  }

  char *buffer = (char *)spdk_zmalloc(data_size, sysconf(_SC_PAGESIZE), 0, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  while (buffer == nullptr) // [DU] retry
  {
      buffer = (char *)spdk_zmalloc(data_size, sysconf(_SC_PAGESIZE), 0, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  }
  memcpy(buffer, data, data_size);

  if (!active_zone_) {
    Zone* zone = nullptr;
    s = zbd_->AllocateZone(lifetime_, &zone);
    if (!s.ok()) return s;

    if (!zone) {
      return IOStatus::NoSpace(
          "Out of space: Zone allocation failure while setting active zone");
    }

    SetActiveZone(zone);
    extent_start_ = active_zone_->wp_;
    extent_filepos_ = fileSize;
  }

  while (left) {
    if (active_zone_->capacity_ == 0) {
      PushExtent();

      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }

      Zone* zone = nullptr;
      s = zbd_->AllocateZone(lifetime_, &zone);
      if (!s.ok()) return s;

      if (!zone) {
        return IOStatus::NoSpace(
            "Out of space: Zone allocation failure while replacing active "
            "zone");
      }

      SetActiveZone(zone);

      extent_start_ = active_zone_->wp_;
      extent_filepos_ = fileSize;
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    s = active_zone_->Write((char*)buffer + offset, wr_size, /*is_wal=*/IsWal());
    if (!s.ok()) return s;

    fileSize += wr_size;
    left -= wr_size;
    offset += wr_size;
  }

  spdk_free(buffer);

  fileSize -= (data_size - valid_size);
  return s;
}

IOStatus ZoneFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime) {
  lifetime_ = lifetime;
  return IOStatus::OK();
}

void ZoneFile::ReleaseActiveZone() {
  assert(active_zone_ != nullptr);
  bool ok = active_zone_->Release();
  assert(ok);
  (void)ok;
  active_zone_ = nullptr;
}

void ZoneFile::SetActiveZone(Zone* zone) {
  assert(active_zone_ == nullptr);
  assert(zone->IsBusy());
  active_zone_ = zone;
}

void ZoneFile::PrepareWalWrite() {
  // Update active_zone_ and offset from zbd
  active_zone_ = zbd_->RetrieveWalZone();
  extent_start_ = active_zone_->wp_;
}

void ZoneFile::UpdateWalExtent() {
  // zone append mode only, zrwa tracks its extents on append
  if (g_wal_zrwa != nullptr) return;

  if (!active_zone_)
    return;

  char *buffer = zbd_->GetWalExtentBuffer();
  IOStatus s = IOStatus::NoSpace();
  uint64_t alba;

  s = active_zone_->Append(buffer, 4096, &alba, /*is_wal=*/true);

  alba *= 4096;

  if (s.ok()) {
    // in the case of append success, alba is the last valid location for this WAL file
    uint64_t length = alba - extent_start_;
    extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_));
    active_zone_->wp_ = alba + 4096; // 4K more increased for this zone
    active_zone_->used_capacity_ += length;
    active_zone_->capacity_ -= (length + 4096);

    fileSize += length;
  } else {
    // in the case of append failure, current zone is fully filled
    uint64_t length = active_zone_->start_ + active_zone_->max_capacity_ - extent_start_;
    extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_));
    active_zone_->wp_ = active_zone_->start_ + active_zone_->max_capacity_; // end pointer
    active_zone_->used_capacity_ += length;
    active_zone_->capacity_ -= length;

    fileSize += length;
  }
}

void ZoneFile::PushZrwaExtent(uint64_t end_pos, Zone* zone) {
  if (end_pos <= extent_start_) return;
  uint64_t length = end_pos - extent_start_;
  extents_.push_back(new ZoneExtent(extent_start_, length, zone));
  zone->used_capacity_ += length;
  extent_start_ = end_pos;
  fileSize += length;
}

void ZoneFile::OnWalZrwaAppend(const void* raw) {
  const ZrwaAppendResult* r = static_cast<const ZrwaAppendResult*>(raw);
  if (zrwa_first_ || r->zone_generation != zrwa_prev_gen_) {
    if (!zrwa_first_) PushZrwaExtent(zrwa_prev_end_pos_, zrwa_prev_zone_);
    extent_start_ = r->start_pos;
    zrwa_first_ = false;
    PersistMetadataIfWal();
  }
  zrwa_prev_end_pos_ = r->end_pos;
  zrwa_prev_zone_ = r->zone;
  zrwa_prev_gen_ = r->zone_generation;
}

void ZoneFile::CloseZrwaExtent() {
  if (!zrwa_first_ && zrwa_prev_zone_) {
    PushZrwaExtent(zrwa_prev_end_pos_, zrwa_prev_zone_);
  }
}

void ZoneFile::PersistMetadataIfWal() {
  if (!IsWal() || metadata_writer_ == nullptr) return;
  metadata_writer_->Persist(shared_from_this());
}

IOStatus ZoneFile::Recover() {
  struct RecoverTimer {
    std::chrono::steady_clock::time_point start;
    RecoverTimer() : start(std::chrono::steady_clock::now()) {}
    ~RecoverTimer() {
      uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start).count();
      g_wal_extent_recover_ns.fetch_add(ns, std::memory_order_relaxed);
      g_wal_extent_recover_count.fetch_add(1, std::memory_order_relaxed);
    }
  } recover_timer;

  /* extent_start_ is the LBA where the in-flight extent began. active_zone_
   * is not persisted, so the zone comes from the in-memory zone map. */
  if (!HasActiveExtent()) return IOStatus::OK();

  Zone* zone = zbd_->GetIOZone(extent_start_);
  if (zone == nullptr) {
    return IOStatus::IOError(
        "Could not find zone for extent start while recovering");
  }

  if (extent_start_ < zone->start_) {
    // corruption guard, clamp to the zone start
    extent_start_ = zone->start_;
  }

  // the write pointer only advances on flush, only WAL files use zrwa
  if (g_wal_zrwa != nullptr && IsWal()) {
    // zone->wp_ is the flushed boundary, the zrwa window past it is durable
    uint64_t hw_wp = zone->wp_;
    uint64_t zone_end = zone->start_ + zone->max_capacity_;
    uint64_t zrwa_bytes =
        static_cast<uint64_t>(g_wal_zrwa->getZrwaSize()) * 4096ULL;
    uint64_t end_pos = hw_wp + zrwa_bytes;
    if (end_pos > zone_end) end_pos = zone_end;

    if (end_pos <= extent_start_) {
      extent_start_ = NO_EXTENT;
      return IOStatus::OK();
    }

    uint64_t length = end_pos - extent_start_;
    extents_.push_back(new ZoneExtent(extent_start_, length, zone));
    zone->wp_ = end_pos;
    zone->used_capacity_ += length;
    if (length <= zone->capacity_) {
      zone->capacity_ -= length;
    } else {
      zone->capacity_ = 0;
    }
    extent_start_ = NO_EXTENT;

    fileSize = 0;
    for (auto* e : extents_) fileSize += e->length_;
    return IOStatus::OK();
  }

  /* Non-WAL files take their size from the MANIFEST and the zone may have
   * been reused, so drop the stale marker instead of absorbing the range. */
  if (!IsWal()) {
    extent_start_ = NO_EXTENT;
    return IOStatus::OK();
  }

  /* The zone was reset once the WAL became obsolete, nothing to recover */
  if (zone->wp_ < extent_start_) {
    extent_start_ = NO_EXTENT;
    return IOStatus::OK();
  }

  uint64_t to_recover = zone->wp_ - extent_start_;
  if (to_recover == 0) {
    extent_start_ = NO_EXTENT;
    return IOStatus::OK();
  }

  zone->used_capacity_ += to_recover;
  extents_.push_back(new ZoneExtent(extent_start_, to_recover, zone));
  extent_start_ = NO_EXTENT;

  fileSize = 0;
  for (auto* e : extents_) fileSize += e->length_;

  return IOStatus::OK();
}

ZonedWritableFile::ZonedWritableFile(ZonedBlockDevice* zbd, bool _buffered,
                                     std::shared_ptr<ZoneFile> zoneFile,
                                     MetadataWriter* metadata_writer) {
  wp = zoneFile->GetFileSize();
  assert(wp == 0);

  buffered = _buffered;
  block_sz = zbd->GetBlockSize();
  //buffer_sz = block_sz * 256;
  buffer_sz = 8192;
  buffer_pos = 0;

  zoneFile_ = zoneFile;

  if (buffered) {
    int ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), buffer_sz);

    if (ret) buffer = nullptr;

    assert(buffer != nullptr);
  } else if (waltz_mode) {
    //zoneFile_->PrepareWalWrite();
  }

  metadata_writer_ = metadata_writer;
  zoneFile_->SetMetadataWriter(metadata_writer);
  zoneFile_->OpenWR();
}

ZonedWritableFile::~ZonedWritableFile() {
  if (zoneFile_->IsSimulatedCrash()) {
    // simulate SIGKILL: free the user-space buffer only
    if (buffered) free(buffer);
    return;
  }
  IOStatus s = zoneFile_->CloseWR();
  if (buffered) free(buffer);

  if (!s.ok()) {
    zoneFile_->GetZbd()->SetZoneDeferredStatus(s);
  }
}

MetadataWriter::~MetadataWriter() {}

IOStatus ZonedWritableFile::Truncate(uint64_t size,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  zoneFile_->SetFileSize(size);
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Fsync(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  if (zoneFile_->IsSimulatedCrash()) {
    // simulate SIGKILL: skip the flush and the metadata persist
    return IOStatus::OK();
  }
  IOStatus s;

  if (waltz_mode == false || buffered) {
    buffer_mtx_.lock();
    s = FlushBuffer();
    buffer_mtx_.unlock();
    if (!s.ok()) {
      return s;
    }
    zoneFile_->PushExtent();
  }

  if (zenfs_skip_meta_sync)
    return IOStatus::OK();
  return metadata_writer_->Persist(zoneFile_);
}

IOStatus ZonedWritableFile::Sync(const IOOptions& options,
                                 IODebugContext* dbg) {
  return Fsync(options, dbg);
}

IOStatus ZonedWritableFile::Flush(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& options,
                                      IODebugContext* dbg) {
  if (wp < offset + nbytes) return Fsync(options, dbg);

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Close(const IOOptions& options,
                                  IODebugContext* dbg) {
  if (zoneFile_->IsSimulatedCrash()) {
    // simulate SIGKILL: skip the sync and the close path
    return IOStatus::OK();
  }
  Fsync(options, dbg);
  if (waltz_mode == true && !buffered) {
    if (g_wal_zrwa != nullptr) {
      // finalize the extent tracked on append
      zoneFile_->CloseZrwaExtent();
    } else {
      // zone append mode
      zoneFile_->UpdateWalExtent();
    }
  }
  return zoneFile_->CloseWR();
}

IOStatus ZonedWritableFile::FlushBuffer() {
  uint32_t align, pad_sz = 0, wr_sz;
  IOStatus s;

  if (!buffer_pos) return IOStatus::OK();

  align = buffer_pos % block_sz;
  if (align) pad_sz = block_sz - align;

  if (pad_sz) memset((char*)buffer + buffer_pos, 0x0, pad_sz);

  wr_sz = buffer_pos + pad_sz;
  s = zoneFile_->Append((char*)buffer, wr_sz, buffer_pos);
  if (!s.ok()) {
    return s;
  }

  wp += buffer_pos;
  buffer_pos = 0;

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::BufferedWrite(const Slice& slice) {
  uint32_t buffer_left = buffer_sz - buffer_pos;
  uint32_t data_left = slice.size();
  char* data = (char*)slice.data();
  uint32_t tobuffer;
  int blocks, aligned_sz;
  int ret;
  void* alignbuf;
  IOStatus s;

  if (buffer_pos || data_left <= buffer_left) {
    if (data_left < buffer_left) {
      tobuffer = data_left;
    } else {
      tobuffer = buffer_left;
    }

    memcpy(buffer + buffer_pos, data, tobuffer);
    buffer_pos += tobuffer;
    data_left -= tobuffer;

    if (!data_left) return IOStatus::OK();

    data += tobuffer;
  }

  if (buffer_pos == buffer_sz) {
    s = FlushBuffer();
    if (!s.ok()) return s;
  }

  if (data_left >= buffer_sz) {
    blocks = data_left / block_sz;
    aligned_sz = block_sz * blocks;

    ret = posix_memalign(&alignbuf, sysconf(_SC_PAGESIZE), aligned_sz);
    if (ret) {
      return IOStatus::IOError("failed allocating alignment write buffer\n");
    }

    memcpy(alignbuf, data, aligned_sz);
    s = zoneFile_->Append(alignbuf, aligned_sz, aligned_sz);
    free(alignbuf);

    if (!s.ok()) return s;

    wp += aligned_sz;
    data_left -= aligned_sz;
    data += aligned_sz;
  }

  if (data_left) {
    memcpy(buffer, data, data_left);
    buffer_pos = data_left;
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Append(const Slice& data,
                                   const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

IOStatus ZonedWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*options*/,
                                             IODebugContext* /*dbg*/) {
  IOStatus s;

  if (offset != wp) {
    assert(false);
    return IOStatus::IOError("positioned append not at write pointer");
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

void ZonedWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  zoneFile_->SetWriteLifeTimeHint(hint);
}

IOStatus ZonedSequentialFile::Read(size_t n, const IOOptions& /*options*/,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  s = zoneFile_->PositionedRead(rp, n, result, scratch, direct_);
  if (s.ok()) rp += result->size();

  return s;
}

IOStatus ZonedSequentialFile::Skip(uint64_t n) {
  if (rp + n >= zoneFile_->GetFileSize())
    return IOStatus::InvalidArgument("Skip beyond end of file");
  rp += n;
  return IOStatus::OK();
}

IOStatus ZonedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& /*options*/,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  return zoneFile_->PositionedRead(offset, n, result, scratch, direct_);
}

IOStatus ZonedRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*options*/,
                                     Slice* result, char* scratch,
                                     IODebugContext* /*dbg*/) const {
  return zoneFile_->PositionedRead(offset, n, result, scratch, direct_);
}

size_t ZoneFile::GetUniqueId(char* id, size_t max_size) {
  /* Based on the posix fs implementation */
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = 0; //fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, file_id_);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);

  return 0;
}

size_t ZonedRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return zoneFile_->GetUniqueId(id, max_size);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
