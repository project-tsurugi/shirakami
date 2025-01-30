#pragma once

#include <memory>
#include <cstdint>
#include <string_view>

#include "concurrency_control/include/epoch.h"

#include "limestone/api/snapshot.h"
#include "limestone/api/storage_id_type.h"
#include <limestone/api/write_version_type.h>

#include "boost/filesystem/path.hpp"

namespace shirakami {

// datastore

limestone::api::log_channel*
create_channel(limestone::api::datastore* ds,
               boost::filesystem::path const& location);

std::unique_ptr<limestone::api::snapshot>
get_snapshot(limestone::api::datastore* ds);

void ready(limestone::api::datastore* ds);

void recover(limestone::api::datastore* ds);

void switch_epoch(limestone::api::datastore* ds, epoch::epoch_t ep);

// log_channel

void add_entry(limestone::api::log_channel* lc,
               limestone::api::storage_id_type storage_id, std::string_view key,
               std::string_view val, limestone::api::epoch_t major_version,
               std::uint64_t minor_version,
               const std::vector<limestone::api::blob_id_type>& large_objects);

void remove_entry(limestone::api::log_channel* lc,
                  limestone::api::storage_id_type storage_id,
                  std::string_view key, limestone::api::epoch_t major_version,
                  std::uint64_t minor_version);

void add_storage(limestone::api::log_channel* lc,
                 limestone::api::storage_id_type storage_id,
                 limestone::api::epoch_t major_version,
                 std::uint64_t minor_version);

void remove_storage(limestone::api::log_channel* lc,
                    limestone::api::storage_id_type storage_id,
                    limestone::api::epoch_t major_version,
                    std::uint64_t minor_version);

void begin_session(limestone::api::log_channel* lc);

void end_session(limestone::api::log_channel* lc);

} // namespace shirakami
