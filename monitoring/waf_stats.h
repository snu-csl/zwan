#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

void ResetWafStats();
void AddUserDataBytes(uint64_t bytes);
void AddWalPayloadBytes(uint64_t bytes);
void AddWalRecordCount(uint64_t count);
void AddWalDeviceBytes(uint64_t bytes);
void AddTotalDeviceBytes(uint64_t bytes);
void AddMetaDeviceBytes(uint64_t bytes);
void PrintWafStats(const char* phase);

}  // namespace ROCKSDB_NAMESPACE
