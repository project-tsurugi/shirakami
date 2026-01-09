
#include <boost/filesystem/path.hpp>
#include <glog/logging.h>
#include <limestone/api/datastore.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <cstdint>
#include <ostream>
#include <memory>
#include <string_view>

#include "database/include/logging.h"
#include "datastore/limestone/include/limestone_api_helper.h"
#include "shirakami/logging.h"
#include "concurrency_control/include/epoch.h"

namespace shirakami {

// datastore

#if HAVE_LIMESTONE_DATASTORE_CREATE_CHANNEL_NONE
limestone::api::log_channel*
create_channel(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return nullptr;
    }
    shirakami_log_entry << "datastore::create_channel()";
    auto& ret = ds->create_channel();
    shirakami_log_exit << "datastore::create_channel(): ret: " << &ret;
    return &ret;
}
#else
limestone::api::log_channel*
create_channel(limestone::api::datastore* ds,
               boost::filesystem::path const& location) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return nullptr;
    }
    shirakami_log_entry << "datastore::create_channel(): location: " << location;
    auto& ret = ds->create_channel(location);
    shirakami_log_exit << "datastore::create_channel(): ret: " << &ret;
    return &ret;
}
#endif

std::unique_ptr<limestone::api::snapshot>
get_snapshot(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return nullptr;
    }
    shirakami_log_entry << "datastore::get_snapshot()";
    auto ret = ds->get_snapshot();
    shirakami_log_exit << "datastore::get_snapshot(): ret: " << ret.get();
    return ret;
}

void ready(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "datastore::ready()";
    ds->ready();
    shirakami_log_exit << "datastore::ready()";
}

void recover(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "datastore::recover()";
    ds->recover();
    shirakami_log_exit << "datastore::recover()";
}

void switch_epoch(limestone::api::datastore* ds, epoch::epoch_t ep) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_ex_log_entry << "datastore::switch_epoch()";
    ds->switch_epoch(ep);
    shirakami_ex_log_exit << "datastore::switch_epoch()";
}

void switch_available_boundary_version(limestone::api::datastore* ds, epoch::epoch_t ep) {
    if (ds == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_ex_log_entry << "datastore::switch_available_boundary_version()";
    ds->switch_available_boundary_version(limestone::api::write_version_type(ep, 0UL));
    shirakami_ex_log_exit << "datastore::switch_available_boundary_version()";
}

// log channel

void add_entry(limestone::api::log_channel* lc,
               limestone::api::storage_id_type storage_id,
               std::string_view const key, std::string_view const val,
               limestone::api::epoch_t major_version,
               std::uint64_t minor_version,
               const std::vector<limestone::api::blob_id_type>& large_objects) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::add_entry(): storage_id: "
                        << storage_id << "," shirakami_binstring(key) "," shirakami_binstring(val)
                           ", major write version: " << major_version
                        << ", minor write version: " << minor_version
                        << "," shirakami_vecstring(large_objects);
#if 0 // NOLINT
    lc->add_entry(
            storage_id, key, val,
            limestone::api::write_version_type(major_version, minor_version),
            large_objects);
#else
    // avoid limestone unimplemented
    if (large_objects.empty()) {
        lc->add_entry(
                storage_id, key, val,
                limestone::api::write_version_type(major_version, minor_version));
    } else {
        lc->add_entry(
                storage_id, key, val,
                limestone::api::write_version_type(major_version, minor_version),
                large_objects);
    }
#endif
    shirakami_log_exit << "log_channel::add_entry()";
}

void remove_entry(limestone::api::log_channel* const lc,
                  limestone::api::storage_id_type const storage_id,
                  std::string_view const key,
                  limestone::api::epoch_t const major_version,
                  std::uint64_t const minor_version) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::remove_entry(): storage_id: "
                        << storage_id << "," shirakami_binstring(key)
                           ", major write version: " << major_version
                        << ", minor write version: " << minor_version;
    lc->remove_entry(
            storage_id, key,
            limestone::api::write_version_type(major_version, minor_version));
    shirakami_log_exit << "log_channel::remove_entry()";
}

void add_storage(limestone::api::log_channel* const lc,
                 limestone::api::storage_id_type const storage_id,
                 limestone::api::epoch_t const major_version,
                 std::uint64_t const minor_version) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::add_storage(): storage_id: " << storage_id
                        << ", major write version: " << major_version
                        << ", minor write version: " << minor_version;
    lc->add_storage(
            storage_id,
            limestone::api::write_version_type(major_version, minor_version));
    shirakami_log_exit << "log_channel::add_storage()";
}

void remove_storage(limestone::api::log_channel* const lc,
                    limestone::api::storage_id_type const storage_id,
                    limestone::api::epoch_t const major_version,
                    std::uint64_t const minor_version) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::remove_storage(): storage_id: " << storage_id
                        << ", major write version: " << major_version
                        << ", minor write version: " << minor_version;
    lc->remove_storage(
            storage_id,
            limestone::api::write_version_type(major_version, minor_version));
    shirakami_log_exit << "log_channel::remove_storage()";
}

void begin_session(limestone::api::log_channel* lc) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::begin_session()";
    lc->begin_session();
    shirakami_log_exit << "log_channel::begin_session()";
}

void end_session(limestone::api::log_channel* lc) {
    if (lc == nullptr) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return;
    }
    shirakami_log_entry << "log_channel::end_session()";
    lc->end_session();
    shirakami_log_exit << "log_channel::end_session()";
}

} // namespace shirakami
