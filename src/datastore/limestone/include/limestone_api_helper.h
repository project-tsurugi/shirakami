#pragma once

#include "limestone/api/datastore.h"
#include "limestone/api/snapshot.h"

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/logging.h"

#include "boost/filesystem/path.hpp"

#include "glog/logging.h"

#define log_entry VLOG(log_trace) << std::boolalpha << "-->" // NOLINT
#define log_exit VLOG(log_trace) << std::boolalpha << "<--"  // NOLINT

namespace shirakami {

// datastore

limestone::api::log_channel*
create_channel(limestone::api::datastore* ds,
               boost::filesystem::path const& location);

limestone::api::snapshot* get_snapshot(limestone::api::datastore* ds);

void ready(limestone::api::datastore* ds);

void recover(limestone::api::datastore* ds, bool overwrite);

void switch_epoch(limestone::api::datastore* ds, epoch::epoch_t ep);

// log_channel

void add_entry(limestone::api::log_channel* lc,
               limestone::api::storage_id_type storage_id, std::string_view key,
               std::string_view val, limestone::api::epoch_t major_version,
               std::uint64_t minor_version);

void begin_session(limestone::api::log_channel* lc);

void end_session(limestone::api::log_channel* lc);

} // namespace shirakami