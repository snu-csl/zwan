//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "file/sequence_file_reader.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(std::shared_ptr<Logger> info_log,
               std::unique_ptr<SequentialFileReader>&& _file,
               Reporter* reporter, bool checksum, uint64_t log_num,
               bool zrwa_mode)
    : info_log_(info_log),
      file_(std::move(_file)),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      read_error_(false),
      eof_offset_(0),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      log_number_(log_num),
      recycled_(false),
      zrwa_mode_(zrwa_mode) {}

Reader::~Reader() {
  delete[] backing_store_;
}

// For kAbsoluteConsistency, on clean shutdown we don't expect any error
// in the log files.  For other modes, we can ignore only incomplete records
// in the last log file, which are presumably due to a write in progress
// during restart (or from log recycling).
//
// TODO krad: Evaluate if we need to move to a more strict mode where we
// restrict the inconsistency to only the last log
bool Reader::ReadRecord(Slice* record, std::string* scratch,
                        WALRecoveryMode wal_recovery_mode) {
  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
    size_t drop_size = 0;
    const unsigned int record_type = ReadPhysicalRecord(&fragment, &drop_size);
    switch (record_type) {
      case kFullType:
      case kRecyclableFullType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(1)");
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
      case kRecyclableFirstType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(2)");
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
      case kRecyclableMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
      case kRecyclableLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kBadHeader:
        if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
            wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
          // In clean shutdown we don't expect any error in the log files.
          // In point-in-time recovery an incomplete record at the end could
          // produce a hole in the recovered data. Report an error here, which
          // higher layers can choose to ignore when it's provable there is no
          // hole.
          ReportCorruption(drop_size, "truncated header");
        }
        FALLTHROUGH_INTENDED;

      case kEof:
        if (in_fragmented_record) {
          if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
              wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
            // In clean shutdown we don't expect any error in the log files.
            // In point-in-time recovery an incomplete record at the end could
            // produce a hole in the recovered data. Report an error here, which
            // higher layers can choose to ignore when it's provable there is no
            // hole.
            ReportCorruption(scratch->size(), "error reading trailing data");
          }
          // This can be caused by the writer dying immediately after
          //  writing a physical record but before completing the next; don't
          //  treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kOldRecord:
        if (wal_recovery_mode != WALRecoveryMode::kSkipAnyCorruptedRecords) {
          // Treat a record from a previous instance of the log as EOF.
          if (in_fragmented_record) {
            if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
                wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
              // In clean shutdown we don't expect any error in the log files.
              // In point-in-time recovery an incomplete record at the end could
              // produce a hole in the recovered data. Report an error here,
              // which higher layers can choose to ignore when it's provable
              // there is no hole.
              ReportCorruption(scratch->size(), "error reading trailing data");
            }
            // This can be caused by the writer dying immediately after
            //  writing a physical record but before completing the next; don't
            //  treat it as a corruption, just ignore the entire logical record.
            scratch->clear();
          }
          return false;
        }
        FALLTHROUGH_INTENDED;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      case kBadRecordLen:
        if (eof_) {
          if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
              wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
            // In clean shutdown we don't expect any error in the log files.
            // In point-in-time recovery an incomplete record at the end could
            // produce a hole in the recovered data. Report an error here, which
            // higher layers can choose to ignore when it's provable there is no
            // hole.
            ReportCorruption(drop_size, "truncated record body");
          }
          return false;
        }
        FALLTHROUGH_INTENDED;

      case kBadRecordChecksum:
        if (recycled_ &&
            wal_recovery_mode ==
                WALRecoveryMode::kTolerateCorruptedTailRecords) {
          scratch->clear();
          return false;
        }
        if (record_type == kBadRecordLen) {
          ReportCorruption(drop_size, "bad record length");
        } else {
          ReportCorruption(drop_size, "checksum mismatch");
        }
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

uint64_t Reader::LastRecordEnd() {
  return end_of_buffer_offset_ - buffer_.size();
}

void Reader::UnmarkEOF() {
  if (read_error_) {
    return;
  }
  eof_ = false;
  if (eof_offset_ == 0) {
    return;
  }
  UnmarkEOFInternal();
}

void Reader::UnmarkEOFInternal() {
  // If the EOF was in the middle of a block (a partial block was read) we have
  // to read the rest of the block as ReadPhysicalRecord can only read full
  // blocks and expects the file position indicator to be aligned to the start
  // of a block.
  //
  //      consumed_bytes + buffer_size() + remaining == kBlockSize

  size_t consumed_bytes = eof_offset_ - buffer_.size();
  size_t remaining = kBlockSize - eof_offset_;

  // backing_store_ is used to concatenate what is left in buffer_ and
  // the remainder of the block. If buffer_ already uses backing_store_,
  // we just append the new data.
  if (buffer_.data() != backing_store_ + consumed_bytes) {
    // Buffer_ does not use backing_store_ for storage.
    // Copy what is left in buffer_ to backing_store.
    memmove(backing_store_ + consumed_bytes, buffer_.data(), buffer_.size());
  }

  Slice read_buffer;
  Status status = file_->Read(remaining, &read_buffer,
    backing_store_ + eof_offset_);

  size_t added = read_buffer.size();
  end_of_buffer_offset_ += added;

  if (!status.ok()) {
    if (added > 0) {
      ReportDrop(added, status);
    }

    read_error_ = true;
    return;
  }

  if (read_buffer.data() != backing_store_ + eof_offset_) {
    // Read did not write to backing_store_
    memmove(backing_store_ + eof_offset_, read_buffer.data(),
      read_buffer.size());
  }

  buffer_ = Slice(backing_store_ + consumed_bytes,
    eof_offset_ + added - consumed_bytes);

  if (added < remaining) {
    eof_ = true;
    eof_offset_ += added;
  } else {
    eof_offset_ = 0;
  }
}

void Reader::ReportCorruption(size_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(size_t bytes, const Status& reason) {
  if (reporter_ != nullptr) {
    reporter_->Corruption(bytes, reason);
  }
}

bool Reader::ReadMore(size_t* drop_size, int *error) {
  if (!eof_ && !read_error_) {
    // Last read was a full read, so this is a trailer to skip
    buffer_.clear();
    Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
    TEST_SYNC_POINT_CALLBACK("LogReader::ReadMore:AfterReadFile", &status);
    end_of_buffer_offset_ += buffer_.size();
    if (!status.ok()) {
      buffer_.clear();
      ReportDrop(kBlockSize, status);
      read_error_ = true;
      *error = kEof;
      return false;
    } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
      eof_ = true;
      eof_offset_ = buffer_.size();
    }
    return true;
  } else {
    // Note that if buffer_ is non-empty, we have a truncated header at the
    //  end of the file, which can be caused by the writer crashing in the
    //  middle of writing the header. Unless explicitly requested we don't
    //  considering this an error, just report EOF.
    if (buffer_.size()) {
      *drop_size = buffer_.size();
      buffer_.clear();
      *error = kBadHeader;
      return false;
    }
    buffer_.clear();
    *error = kEof;
    return false;
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result, size_t* drop_size) {
  if (zrwa_mode_) {
    return ReadZrwaPhysicalRecord(result, drop_size);
  }
  while (true) {
    // We need at least the minimum header size
    if (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
      // the default value of r is meaningless because ReadMore will overwrite
      // it if it returns false; in case it returns true, the return value will
      // not be used anyway
      int r = kEof;
      if (!ReadMore(drop_size, &r)) {
        return r;
      }
      continue;
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    int header_size = kHeaderSize;
    if (type >= kRecyclableFullType && type <= kRecyclableLastType) {
      if (end_of_buffer_offset_ - buffer_.size() == 0) {
        recycled_ = true;
      }
      header_size = kRecyclableHeaderSize;
      // We need enough for the larger header
      if (buffer_.size() < static_cast<size_t>(kRecyclableHeaderSize)) {
        int r = kEof;
        if (!ReadMore(drop_size, &r)) {
          return r;
        }
        continue;
      }
      const uint32_t log_num = DecodeFixed32(header + 7);
      if (log_num != log_number_) {
        return kOldRecord;
      }
    }
    if (header_size + length > buffer_.size()) {
      assert(buffer_.size() >= static_cast<size_t>(header_size));
      *drop_size = buffer_.size();
      buffer_.clear();
      // If the end of the read has been reached without seeing
      // `header_size + length` bytes of payload, report a corruption. The
      // higher layers can decide how to handle it based on the recovery mode,
      // whether this occurred at EOF, whether this is the final WAL, etc.
      return kBadRecordLen;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // NOTE: this should never happen in DB written by new RocksDB versions,
      // since we turn off mmap writes to manifest and log files
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        *drop_size = buffer_.size();
        buffer_.clear();
        return kBadRecordChecksum;
      }
    }

    buffer_.remove_prefix(header_size + length);

    *result = Slice(header + header_size, length);
    return type;
  }
}

// ZRWA page is [page_crc 4B] followed by [len 2B|type 1B|payload] records
// and a zero pad. The page CRC covers the records, which never cross a page.
unsigned int Reader::ReadZrwaPhysicalRecord(Slice* result, size_t* drop_size) {
  constexpr size_t kZrwaPageSize = 4096;
  constexpr size_t kZrwaPageHdr = 4;
  constexpr size_t kZrwaRecHdr = 3;

  while (true) {
    // A refill can hold several pages, validate and consume one at a time
    uint64_t cur_off = end_of_buffer_offset_ - buffer_.size();
    size_t off_in_page = static_cast<size_t>(cur_off % kZrwaPageSize);

    if (off_in_page == 0) {
      // At a page boundary the page CRC and one header must be present
      while (buffer_.size() < kZrwaPageHdr + kZrwaRecHdr) {
        int r = kEof;
        if (!ReadMore(drop_size, &r)) {
          *drop_size = buffer_.size();
          buffer_.clear();
          return r;
        }
      }

      // Validate the page CRC within this page
      size_t page_bytes = std::min(buffer_.size(), kZrwaPageSize);
      uint32_t expected = crc32c::Unmask(DecodeFixed32(buffer_.data()));
      const char* p = buffer_.data() + kZrwaPageHdr;
      const char* page_end = buffer_.data() + page_bytes;
      uint32_t actual = 0;
      while (p + kZrwaRecHdr <= page_end) {
        uint32_t len = (static_cast<uint32_t>(static_cast<uint8_t>(p[0]))) |
                       (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8);
        if (len == 0) break;
        if (p + kZrwaRecHdr + len > page_end) break;
        actual = crc32c::Extend(actual, p, kZrwaRecHdr + len);
        p += kZrwaRecHdr + len;
      }

      if (checksum_ && actual != expected) {
        // Page never persisted or corrupt, treat as EOF and drop the tail
        *drop_size = buffer_.size();
        buffer_.clear();
        return kEof;
      }

      // Consume the page header, buffer_ then points at the first record
      buffer_.remove_prefix(kZrwaPageHdr);
      cur_off = end_of_buffer_offset_ - buffer_.size();
      off_in_page = static_cast<size_t>(cur_off % kZrwaPageSize);
    }

    // Bytes remaining in this page that can still hold records.
    size_t bytes_left_in_page = kZrwaPageSize - off_in_page;
    size_t avail = std::min(buffer_.size(), bytes_left_in_page);
    if (avail < kZrwaRecHdr) {
      // No room for another header in this page, move to the next page
      size_t skip = std::min(bytes_left_in_page, buffer_.size());
      buffer_.remove_prefix(skip);
      continue;
    }

    const char* h = buffer_.data();
    uint32_t len = (static_cast<uint32_t>(static_cast<uint8_t>(h[0]))) |
                   (static_cast<uint32_t>(static_cast<uint8_t>(h[1])) << 8);
    if (len == 0) {
      // Zero pad, skip the rest of this page
      size_t skip = std::min(bytes_left_in_page, buffer_.size());
      buffer_.remove_prefix(skip);
      continue;
    }
    if (kZrwaRecHdr + len > avail) {
      // Record runs past the page end, treat as a bad record
      *drop_size = buffer_.size();
      buffer_.clear();
      return kBadRecordLen;
    }
    unsigned int type = static_cast<uint8_t>(h[2]);

    // Batches carry a single Put, so recovery re-prepends count=1
    zrwa_record_buf_.resize(4 + len);
    EncodeFixed32(&zrwa_record_buf_[0], 1u);
    memcpy(&zrwa_record_buf_[4], h + kZrwaRecHdr, len);
    *result = Slice(zrwa_record_buf_.data(), zrwa_record_buf_.size());

    buffer_.remove_prefix(kZrwaRecHdr + len);
    return type;
  }
}

bool FragmentBufferedReader::ReadRecord(Slice* record, std::string* scratch,
                                        WALRecoveryMode /*unused*/) {
  assert(record != nullptr);
  assert(scratch != nullptr);
  record->clear();
  scratch->clear();

  uint64_t prospective_record_offset = 0;
  uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
  size_t drop_size = 0;
  unsigned int fragment_type_or_err = 0;  // Initialize to make compiler happy
  Slice fragment;
  while (TryReadFragment(&fragment, &drop_size, &fragment_type_or_err)) {
    switch (fragment_type_or_err) {
      case kFullType:
      case kRecyclableFullType:
        if (in_fragmented_record_ && !fragments_.empty()) {
          ReportCorruption(fragments_.size(), "partial record without end(1)");
        }
        fragments_.clear();
        *record = fragment;
        prospective_record_offset = physical_record_offset;
        last_record_offset_ = prospective_record_offset;
        in_fragmented_record_ = false;
        return true;

      case kFirstType:
      case kRecyclableFirstType:
        if (in_fragmented_record_ || !fragments_.empty()) {
          ReportCorruption(fragments_.size(), "partial record without end(2)");
        }
        prospective_record_offset = physical_record_offset;
        fragments_.assign(fragment.data(), fragment.size());
        in_fragmented_record_ = true;
        break;

      case kMiddleType:
      case kRecyclableMiddleType:
        if (!in_fragmented_record_) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          fragments_.append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
      case kRecyclableLastType:
        if (!in_fragmented_record_) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          fragments_.append(fragment.data(), fragment.size());
          scratch->assign(fragments_.data(), fragments_.size());
          fragments_.clear();
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          in_fragmented_record_ = false;
          return true;
        }
        break;

      case kBadHeader:
      case kBadRecord:
      case kEof:
      case kOldRecord:
        if (in_fragmented_record_) {
          ReportCorruption(fragments_.size(), "error in middle of record");
          in_fragmented_record_ = false;
          fragments_.clear();
        }
        break;

      case kBadRecordChecksum:
        if (recycled_) {
          fragments_.clear();
          return false;
        }
        ReportCorruption(drop_size, "checksum mismatch");
        if (in_fragmented_record_) {
          ReportCorruption(fragments_.size(), "error in middle of record");
          in_fragmented_record_ = false;
          fragments_.clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u",
                 fragment_type_or_err);
        ReportCorruption(
            fragment.size() + (in_fragmented_record_ ? fragments_.size() : 0),
            buf);
        in_fragmented_record_ = false;
        fragments_.clear();
        break;
      }
    }
  }
  return false;
}

void FragmentBufferedReader::UnmarkEOF() {
  if (read_error_) {
    return;
  }
  eof_ = false;
  UnmarkEOFInternal();
}

bool FragmentBufferedReader::TryReadMore(size_t* drop_size, int* error) {
  if (!eof_ && !read_error_) {
    // Last read was a full read, so this is a trailer to skip
    buffer_.clear();
    Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
    end_of_buffer_offset_ += buffer_.size();
    if (!status.ok()) {
      buffer_.clear();
      ReportDrop(kBlockSize, status);
      read_error_ = true;
      *error = kEof;
      return false;
    } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
      eof_ = true;
      eof_offset_ = buffer_.size();
      TEST_SYNC_POINT_CALLBACK(
          "FragmentBufferedLogReader::TryReadMore:FirstEOF", nullptr);
    }
    return true;
  } else if (!read_error_) {
    UnmarkEOF();
  }
  if (!read_error_) {
    return true;
  }
  *error = kEof;
  *drop_size = buffer_.size();
  if (buffer_.size() > 0) {
    *error = kBadHeader;
  }
  buffer_.clear();
  return false;
}

// return true if the caller should process the fragment_type_or_err.
bool FragmentBufferedReader::TryReadFragment(
    Slice* fragment, size_t* drop_size, unsigned int* fragment_type_or_err) {
  assert(fragment != nullptr);
  assert(drop_size != nullptr);
  assert(fragment_type_or_err != nullptr);

  while (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
    size_t old_size = buffer_.size();
    int error = kEof;
    if (!TryReadMore(drop_size, &error)) {
      *fragment_type_or_err = error;
      return false;
    } else if (old_size == buffer_.size()) {
      return false;
    }
  }
  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
  const unsigned int type = header[6];
  const uint32_t length = a | (b << 8);
  int header_size = kHeaderSize;
  if (type >= kRecyclableFullType && type <= kRecyclableLastType) {
    if (end_of_buffer_offset_ - buffer_.size() == 0) {
      recycled_ = true;
    }
    header_size = kRecyclableHeaderSize;
    while (buffer_.size() < static_cast<size_t>(kRecyclableHeaderSize)) {
      size_t old_size = buffer_.size();
      int error = kEof;
      if (!TryReadMore(drop_size, &error)) {
        *fragment_type_or_err = error;
        return false;
      } else if (old_size == buffer_.size()) {
        return false;
      }
    }
    const uint32_t log_num = DecodeFixed32(header + 7);
    if (log_num != log_number_) {
      *fragment_type_or_err = kOldRecord;
      return true;
    }
  }

  while (header_size + length > buffer_.size()) {
    size_t old_size = buffer_.size();
    int error = kEof;
    if (!TryReadMore(drop_size, &error)) {
      *fragment_type_or_err = error;
      return false;
    } else if (old_size == buffer_.size()) {
      return false;
    }
  }

  if (type == kZeroType && length == 0) {
    buffer_.clear();
    *fragment_type_or_err = kBadRecord;
    return true;
  }

  if (checksum_) {
    uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
    uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);
    if (actual_crc != expected_crc) {
      *drop_size = buffer_.size();
      buffer_.clear();
      *fragment_type_or_err = kBadRecordChecksum;
      return true;
    }
  }

  buffer_.remove_prefix(header_size + length);

  *fragment = Slice(header + header_size, length);
  *fragment_type_or_err = type;
  return true;
}

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
