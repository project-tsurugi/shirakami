//
// Created by thawk on 2020/11/05.
//

#include <boost/filesystem.hpp>

#include "log.h"

// about cc
#include "concurrency_control/silo_variant/include/record.h"

using namespace shirakami;

// about index
#include "yakushima/include/kvs.h"

#if defined(CPR)

#include "cpr.h"
#include "logger.h"

#elif defined(PWAL)

#include "pwal.h"
using namespace shirakami::pwal;

#endif


namespace shirakami {

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
               logfile.read(reinterpret_cast<void*>(&log_header),  // NOLINT
                            sizeof(LogHeader))) {
            std::vector<LogRecord> log_tmp_buf;
            for (unsigned int j = 0; j < log_header.get_log_rec_num(); ++j) {
                if (fix_size != logfile.read(static_cast<void*>(&log), fix_size)) break;
                std::unique_ptr<char[]> key_ptr;    // NOLINT
                std::unique_ptr<char[]> value_ptr;  // NOLINT
                std::size_t key_length{};
                std::size_t value_length{};
                // read key_length
                if (sizeof(std::size_t) != logfile.read(static_cast<void*>(&key_length),
                                                        sizeof(std::size_t))) {
                    break;
                }
                // read key_body
                if (key_length > 0) {
                    key_ptr = std::make_unique<char[]>(key_length);  // NOLINT
                    if (key_length !=
                        logfile.read(static_cast<void*>(key_ptr.get()), key_length)) {
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
                    value_ptr = std::make_unique<char[]>(value_length);  // NOLINT
                    if (value_length !=
                        logfile.read(static_cast<void*>(value_ptr.get()), value_length)) {
                        break;
                    }
                }

                log_header.set_checksum(log_header.get_checksum() +
                                        log.compute_checksum());
                log_tmp_buf.emplace_back(std::move(log));
            }
            if (log_header.get_checksum() == 0) {
                for (auto &&itr : log_tmp_buf) {
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
    for (auto &&itr : log_set) {
        if (itr.get_tid().get_epoch() > recovery_epoch) break;
        if (itr.get_op() == OP_TYPE::UPDATE || itr.get_op() == OP_TYPE::INSERT) {
            upsert(s, itr.get_tuple()->get_key(), itr.get_tuple()->get_value());
        } else if (itr.get_op() == OP_TYPE::DELETE) {
            delete_record(s, itr.get_tuple()->get_key());
        }
        commit(s); // NOLINT
    }
    leave(s);

#elif defined(CPR)
    // check whether checkpoint file exists.
    boost::system::error_code ec;
    const bool find_result = boost::filesystem::exists(cpr::get_checkpoint_path(), ec);
    if (!find_result || ec) {
        SPDLOG_DEBUG("no checkpoint file to recover.");
        return;
    }
    SPDLOG_DEBUG("checkpoint file to recover exists.");

    std::ifstream logf;
    logf.open(cpr::get_checkpoint_path(), std::ios_base::in | std::ios_base::binary);

    std::string buffer{std::istreambuf_iterator<char>(logf), std::istreambuf_iterator<char>()}; // NOLINT
    size_t offset{0};
    cpr::log_records restore;

    for (;;) {
        if (offset == buffer.size()) break;
        try {
            auto oh = msgpack::unpack(buffer.data(), buffer.size(), offset); // NOLINT
            auto obj = oh.get();
            obj.convert(restore);
        } catch (const std::bad_cast &e) {
            SPDLOG_DEBUG("cast error.");
            exit(1);
        } catch (...) {
            SPDLOG_DEBUG("unknown error.");
            exit(1);
        }

        // recover from restore
        std::vector<cpr::log_record> &logs = restore.get_vec();
        for (auto &&elem : logs) {
            Record* rec_ptr = new Record(elem.get_key(), elem.get_val()); // NOLINT
            rec_ptr->get_tidw() = 0;
            yakushima::status insert_result{yakushima::put<Record*>(elem.get_key(), &rec_ptr)}; // NOLINT
            if (insert_result != yakushima::status::OK) {
                SPDLOG_DEBUG("cpr recovery error.");
                exit(1);
            }
        }
    }

#endif
}

} // namespace shirakami
