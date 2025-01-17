#include <algorithm>

#include "boost/filesystem/path.hpp"

#include "sequence.h"
#include "storage.h"

#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"
#include "shirakami/scheme.h"

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "glog/logging.h"

namespace shirakami::datastore {

#if defined(PWAL)

void init_about_session_table(std::string_view log_dir_path) {
    boost::filesystem::path log_dir{std::string(log_dir_path)};
    for (auto&& elem : session_table::get_session_table()) {
        elem.get_lpwal_handle().set_log_channel_ptr(
                create_channel(get_datastore(), log_dir));
    }
}

void recovery_storage_meta(std::vector<Storage>& st_list) {
    std::sort(st_list.begin(), st_list.end());
    st_list.erase(std::unique(st_list.begin(), st_list.end()), st_list.end());
    if (st_list.back() >= (storage::initial_strg_ctr << 32)) { // LINT
        storage::set_strg_ctr((st_list.back() >> 32) + 1);     // LINT
    } else {
        storage::set_strg_ctr(storage::initial_strg_ctr);
    }
}

void recovery_from_datastore() {
    auto ss = get_snapshot(get_datastore());

    /**
     * The cursor point the first entry at calling first next().
     */
    std::vector<Storage> st_list{};

    auto cursor = ss->get_cursor();
    SequenceId max_id{0};
    while (cursor->next()) { // the next body is none.
        Storage st{cursor->storage()};
        std::string key{};
        std::string val{};
        cursor->key(key);
        cursor->value(val);
        // prepare function updating information
        auto put_data = [](Storage st, std::string_view key,
                           std::string_view val) {
            // check record existence
            Record* rec_ptr{};
            if (Status::OK == get<Record>(st, key, rec_ptr)) {
                // record existing, update value
                rec_ptr->set_value(val);
            } else {
                // create record
                rec_ptr = new Record(key); // LINT
                // fix record contents
                // about value
                rec_ptr->set_value(val);
                // about tid
                tid_word new_tid{rec_ptr->get_tidw_ref()};
                new_tid.set_latest(true);
                new_tid.set_absent(false);
                new_tid.set_lock(false);
                // set tid
                rec_ptr->set_tid(new_tid);
                // put contents to tree
                Token token{};
                // acquire tx handle
                while (enter(token) != Status::OK) { _mm_pause(); }
                yakushima::node_version64* dummy{};
                auto rc = put<Record>(
                        static_cast<session*>(token)->get_yakushima_token(), st,
                        key, rec_ptr, dummy);
                if (yakushima::status::OK != rc) {
                    // can't put
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                          << "unreachable path: " << rc;
                }
                // cleanup
                leave(token);
            }
        };
        // check storage
        if (st == storage::meta_storage) {
            // recovery storage. The storage may have not been operated.
            Storage st2{};
            if (val.size() < (sizeof(st2) + sizeof(storage_option::id_t))) {
                // val size < Storage + id_t + payload
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unreachable path";
                return;
            }
            memcpy(&st2, val.data(), sizeof(st2));
            storage_option::id_t id{};
            memcpy(&id, val.data() + sizeof(st2), sizeof(id)); // LINT
            std::string payload{};
            if (val.size() > sizeof(st2) + sizeof(id)) {
                payload.append(val.data() + sizeof(st2) + sizeof(id), // LINT
                               val.size() - sizeof(st2) - sizeof(id));
            }
            std::string new_value{};
            new_value.append(reinterpret_cast<const char*>(&st2), // LINT
                             sizeof(st2));
            new_value.append(reinterpret_cast<const char*>(&id), // LINT
                             sizeof(id));
            new_value.append(payload);
            // check st2 existence
            auto ret = shirakami::storage::exist_storage(st2);
            if (ret == Status::OK) {
                /**
                 * There must be normal entry for st2 and it was already
                 * processed.
                 * Try to create in-memory entry about storage info and update
                 * key_handle_map.
                 */
                put_data(storage::meta_storage, key, new_value);
                if (storage::key_handle_map_push_storage(key, st2) !=
                    Status::OK) {
                    // Does DML create key handle map entry?
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                          << "library programming error.";
                    return;
                }
            } else {
                // not exist, so create.
                auto ret = shirakami::storage::register_storage(st2,
                                                                {id, payload});
                if (ret != Status::OK) {
                    /**
                     * This process was done because
                     * shirakami::storage::exist_storage(st2) said not exist,
                     * but it can't register_storage.
                     */
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                          << "library programming error";
                    return;
                }
                if (storage::key_handle_map_push_storage(key, st2) !=
                    Status::OK) {
                    /**
                     * This process was done because
                     * shirakami::register_storage(st2, {id, payload}) was
                     * succeeded but it can't create entry of this map.
                     */
                    LOG_FIRST_N(ERROR, 1) << log_location_prefix
                                          << "library programming error";
                    return;
                }
                put_data(storage::meta_storage, key, new_value);
            }
            st_list.emplace_back(st2);
        } else if (st == storage::sequence_storage) {
            // compute sequence id
            SequenceId id{};
            memcpy(&id, key.data(), key.size());
            if (id > max_id) { max_id = id; }
            SequenceVersion version{};
            memcpy(&version, val.data(), sizeof(version));
            SequenceValue value{};
            memcpy(&value, val.data() + sizeof(version), // LINT
                   sizeof(version));
            auto ret = sequence::sequence_map_push(id, 0, version, value);
            if (ret != Status::OK) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "library programming error";
                return;
            }
        } else {
            shirakami::storage::register_storage(st); // maybe already exist
            st_list.emplace_back(st);
            put_data(st, key, val);
        }
    }
    if (max_id > 0) {
        // recovery sequence id generator
        sequence::id_generator_ctr().store(max_id + 1,
                                           std::memory_order_release);
    }
    // recovery storage meta
    if (!st_list.empty()) { recovery_storage_meta(st_list); }
    // recovery epoch info
}

void scan_all_and_logging() {
    // check all storage list
    std::vector<Storage> st_list{};
    storage::list_storage(st_list);

    // logging for all storage
    for (auto&& each_st : st_list) {
        // scan for index
        std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
        std::vector<std::pair<yakushima::node_version64_body,
                              yakushima::node_version64*>>
                nvec;
        auto rc = scan(each_st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                       0, scan_res, &nvec, false);
        if (rc == Status::OK) {
            // It found some records
            for (auto&& each_rec : scan_res) {
                Record* rec_ptr{reinterpret_cast<Record*>( // LINT
                        std::get<1>(each_rec))};
                // get key val info.
                std::string key{};
                rec_ptr->get_key(key);
                std::string val{};
                rec_ptr->get_value(val);
            }
        }
    }
}

#endif

} // namespace shirakami::datastore
