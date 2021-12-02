//
// Created by thawk on 2020/11/05.
//

#include <glog/logging.h>

#include <boost/filesystem.hpp>

#include <algorithm>
#include <vector>

#include "include/log.h"
#include "sequence.h"
#include "storage.h"

// about cc
#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"

#endif

// about index
#include "yakushima/include/kvs.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#elif defined(PWAL)

#include "pwal.h"
using namespace shirakami::pwal;

#endif

using namespace shirakami;

namespace shirakami {

/**
 * @brief todo
 * @pre @a used_storage is not empty.
 * @param [in] used_storage 
 */
void storage_ctr_adjust(std::vector<Storage>& used_storage) {
    // remove duplicates
    std::sort(used_storage.begin(), used_storage.end());
    used_storage.erase(std::unique(used_storage.begin(), used_storage.end()),
                       used_storage.end());

    storage::set_strg_ctr(used_storage.back() + 1);

    std::size_t st_ct{storage::initial_strg_ctr};
    std::unique_lock{storage::get_mt_reuse_num()}; // NOLINT
    for (auto&& elem : used_storage) {
        for (;;) {
            if (elem == st_ct) {
                ++st_ct;
                break;
            }
            storage::get_reuse_num().emplace_back(st_ct);
            ++st_ct;
        }
    }
}

[[maybe_unused]] void Log::recovery_from_log() {
#if defined(PWAL)
    std::vector<LogRecord> log_set;
    for (auto i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        File logfile{};
        std::string filename(kLogDirectory);
        filename.append("/log");
        filename.append(std::to_string(i));
        if (!logfile.try_open(filename, O_RDONLY)) {
            /**
             * the file doesn't exist.
             */
            continue;
        }

        LogRecord log{};
        LogHeader log_header{};
        std::vector<Tuple> tuple_buffer;

        const std::size_t fix_size = sizeof(tid_word) + sizeof(OP_TYPE);
        while (sizeof(LogHeader) ==
               logfile.read(reinterpret_cast<void*>(&log_header), // NOLINT
                            sizeof(LogHeader))) {
            std::vector<LogRecord> log_tmp_buf;
            for (unsigned int j = 0; j < log_header.get_log_rec_num(); ++j) {
                if (fix_size !=
                    logfile.read(static_cast<void*>(&log), fix_size))
                    break;
                std::unique_ptr<char[]> key_ptr;   // NOLINT
                std::unique_ptr<char[]> value_ptr; // NOLINT
                std::size_t key_length{};
                std::size_t value_length{};
                // read key_length
                if (sizeof(std::size_t) !=
                    logfile.read(static_cast<void*>(&key_length),
                                 sizeof(std::size_t))) {
                    break;
                }
                // read key_body
                if (key_length > 0) {
                    key_ptr = std::make_unique<char[]>(key_length); // NOLINT
                    if (key_length !=
                        logfile.read(static_cast<void*>(key_ptr.get()),
                                     key_length)) {
                        break;
                    }
                }
                // read value_length
                if (sizeof(std::size_t) !=
                    logfile.read(static_cast<void*>(&value_length),
                                 sizeof(std::size_t))) {
                    break;
                }
                // read value_body
                if (value_length > 0) {
                    value_ptr =
                            std::make_unique<char[]>(value_length); // NOLINT
                    if (value_length !=
                        logfile.read(static_cast<void*>(value_ptr.get()),
                                     value_length)) {
                        break;
                    }
                }

                log_header.set_checksum(log_header.get_checksum() +
                                        log.compute_checksum());
                log_tmp_buf.emplace_back(std::move(log));
            }
            if (log_header.get_checksum() == 0) {
                for (auto&& itr : log_tmp_buf) {
                    log_set.emplace_back(std::move(itr));
                }
            } else {
                break;
            }
        }

        logfile.close();
    }

    /**
     * If no log files exist, it return.
     */
    if (log_set.empty()) return;

    sort(log_set.begin(), log_set.end());
    const epoch::epoch_t recovery_epoch =
            log_set.back().get_tid().get_epoch() - 2;

    Token s{};
    enter(s);
    for (auto&& itr : log_set) {
        if (itr.get_tid().get_epoch() > recovery_epoch) break;
        if (itr.get_op() == OP_TYPE::UPDATE ||
            itr.get_op() == OP_TYPE::INSERT) {
            upsert(s, itr.get_tuple()->get_key(), itr.get_tuple()->get_value());
        } else if (itr.get_op() == OP_TYPE::DELETE) {
            delete_record(s, itr.get_tuple()->get_key());
        }
        commit(s); // NOLINT
    }
    leave(s);

#elif defined(CPR)
    std::vector<Storage> used_storage;
    auto process_from_file = [&used_storage](const std::string& fname) {
        std::ifstream logf;
        logf.open(fname, std::ios_base::in | std::ios_base::binary);

        std::string buffer{std::istreambuf_iterator<char>(logf),
                           std::istreambuf_iterator<char>()}; // NOLINT
        size_t offset{0};
        cpr::log_records restore;

        yakushima::Token token{};
        yakushima::enter(token);
        for (;;) {
            if (offset == buffer.size()) break;
            try {
                auto oh = msgpack::unpack(buffer.data(), buffer.size(),
                                          offset); // NOLINT
                auto obj = oh.get();
                obj.convert(restore);
            } catch (const std::bad_cast& e) {
                LOG(FATAL) << "cast error";
            } catch (...) { LOG(FATAL) << "unknown error"; }

            // recover from restore
            // note : attention please when impl multi thread
            std::vector<cpr::log_record>& logs = restore.get_vec();
            for (auto&& elem : logs) {
                using namespace yakushima;
                create_storage(elem.get_storage()); // it may already exist.
                Storage st{};
                memcpy(&st, elem.get_storage().data(), sizeof(st));
                used_storage.emplace_back(st);
                Record** result_search = std::get<0>(
                        get<Record*>(elem.get_storage(), elem.get_key()));
                Record* existing_record{nullptr};
                if (result_search != nullptr) {
                    existing_record = *result_search;
                }
                if (!elem.get_delete_op()) {
                    if (existing_record != nullptr) {
                        existing_record->get_tuple().get_pimpl()->set_value(
                                elem.get_val().data(), elem.get_val().size());
                    } else {
                        Record* rec_ptr = new Record(elem.get_key(), // NOLINT
                                                     elem.get_val());
                        rec_ptr->get_tidw() = 0;
                        put<Record*>(token, elem.get_storage(), elem.get_key(),
                                     &rec_ptr);
                    }
                } else if (existing_record != nullptr) {
                    remove(token, elem.get_storage(), elem.get_key());
                }
            }

            // recover from restore about sequence
            std::vector<cpr::log_record_of_seq>& logs_seq =
                    restore.get_vec_seq();
            for (auto&& elem : logs_seq) {
                std::unique_lock lock{sequence_map::get_sm_mtx()};
                /**
                 * Since the values ​​are aggregated for each generation, you can easily overwrite them.
                 */
                sequence_map::get_sm()[elem.get_id()] =
                        sequence_map::value_type{elem.get_val(), elem.get_val(),
                                                 0};
            }

            /**
              * else. In no logging mode, durable value is nothing.
              */
        }
        yakushima::leave(token);
    };

    // check whether checkpoint file exists.
    boost::system::error_code ec;
    const bool find_result =
            boost::filesystem::exists(cpr::get_checkpoint_path(), ec);
    if (!find_result || ec) {
        LOG(INFO) << "no checkpoint file to recover.";
    } else {
        LOG(INFO) << "checkpoint file to recover exists.";
    }

    // for sst files
    using namespace boost::filesystem;
    path dir_path{Log::get_kLogDirectory()};
    directory_iterator end_itr{};
    std::vector<std::pair<cpr::version_type, std::string>> files;
    for (directory_iterator itr(dir_path); itr != end_itr; ++itr) {
        cpr::version_type vnum{stoull(
                std::string(itr->path().string())
                        .erase(0, std::string(Log::get_kLogDirectory()).size() +
                                          4))};
        files.emplace_back(vnum, itr->path().string());
    }
    std::sort(files.begin(), files.end());

    for (auto&& elem : files) { process_from_file(std::get<1>(elem)); }

    if (!used_storage.empty()) {
        // update storage::strg_ctr_, reuse_num_
        storage_ctr_adjust(used_storage);
    }
#endif
}

} // namespace shirakami
