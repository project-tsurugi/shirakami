
#include "datastore/limestone/include/limestone_api_helper.h"

#include "shirakami/scheme.h"

namespace shirakami {

// datastore

limestone::api::log_channel*
create_channel(limestone::api::datastore* ds,
               boost::filesystem::path const& location) {
    if (ds == nullptr) {
        LOG(ERROR) << "programming error";
        return nullptr;
    }
    //log_entry << "datastore::create_channel(): " << location;
    auto& ret = ds->create_channel(location);
    //log_exit << "datastore::create_channel()";
    return &ret;
}

limestone::api::snapshot* get_snapshot(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG(ERROR) << "programming error";
        return nullptr;
    }
    //log_entry << "datastore::get_snapshot()";
    auto* ret = ds->get_snapshot();
    //log_exit << "datastore::get_snapshot(): ret: " << ret;
    return ret;
}

void ready(limestone::api::datastore* ds) {
    if (ds == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    log_entry << "datastore::ready()";
    ds->ready();
    log_exit << "datastore::ready()";
}

void recover(limestone::api::datastore* ds, bool overwrite) {
    if (ds == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    log_entry << "datastore::recover(): overwrite: " << overwrite;
    ds->recover(overwrite);
    log_exit << "datastore::recover()";
}

void switch_epoch(limestone::api::datastore* ds, epoch::epoch_t ep) {
    if (ds == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    //log_entry << "datastore::switch_epoch()";
    ds->switch_epoch(ep);
    //log_exit << "datastore::switch_epoch()";
}

// log channel

void add_entry(limestone::api::log_channel* lc,
               limestone::api::storage_id_type storage_id, std::string_view key,
               std::string_view val, limestone::api::epoch_t major_version,
               std::uint64_t minor_version) {
    if (lc == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    log_entry << "log_channel::add_entry(): storage_id: " << storage_id
              << ", key: " << key << ", val: " << val
              << ", major write version: " << major_version
              << ", minor write version: " << minor_version;
    lc->add_entry(
            storage_id, key, val,
            limestone::api::write_version_type(major_version, minor_version));
    log_exit << "log_channel::add_entry()";
}

void begin_session(limestone::api::log_channel* lc) {
    if (lc == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    //log_entry << "log_channel::begin_session()";
    lc->begin_session();
    //log_exit << "log_channel::begin_session()";
}

void end_session(limestone::api::log_channel* lc) {
    if (lc == nullptr) {
        LOG(ERROR) << "programming error";
        return;
    }
    //log_entry << "log_channel::end_session()";
    lc->end_session();
    //log_exit << "log_channel::end_session()";
}

} // namespace shirakami