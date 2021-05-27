/**
 * @file thread_info.cpp
 * @brief about scheme
 */

#include "concurrency_control/include/session_info.h"
#include "concurrency_control/include/garbage_manager.h"
#include "concurrency_control/include/snapshot_manager.h"

#include "tuple_local.h" // sizeof(Tuple)

namespace shirakami {

void session_info::clean_up_ops_set() {
    read_set.clear();
    read_only_tuples_.clear();
    write_set.clear();
    node_set.clear();
}

void session_info::clean_up_scan_caches() {
    scan_handle_.get_scan_cache().clear();
    scan_handle_.get_scan_cache_itr().clear();
}

[[maybe_unused]] void session_info::display_read_set() {
    std::cout << "==========" << std::endl;
    std::cout << "start : session_info::display_read_set()" << std::endl;
    std::size_t ctr(1);
    for (auto&& itr : read_set) {
        std::cout << "Element #" << ctr << " of read set." << std::endl;
        std::cout << "rec_ptr_ : " << itr.get_rec_ptr() << std::endl;
        Record& record = itr.get_rec_read();
        Tuple& tuple = record.get_tuple();
        std::cout << "tidw_ :vv" << record.get_tidw() << std::endl;
        std::string_view key_view;
        std::string_view value_view;
        key_view = tuple.get_key();
        value_view = tuple.get_value();
        std::cout << "key : " << key_view << std::endl;
        std::cout << "key_size : " << key_view.size() << std::endl;
        std::cout << "value : " << value_view << std::endl;
        std::cout << "value_size : " << value_view.size() << std::endl;
        std::cout << "----------" << std::endl;
        ++ctr;
    }
    std::cout << "==========" << std::endl;
}

[[maybe_unused]] void session_info::display_write_set() {
    std::cout << "==========" << std::endl;
    std::cout << "start : session_info::display_write_set()" << std::endl;
    std::size_t ctr(1);
    for (auto&& itr : write_set) {
        std::cout << "Element #" << ctr << " of write set." << std::endl;
        std::cout << "rec_ptr_ : " << itr.get_rec_ptr() << std::endl;
        std::cout << "op_ : " << itr.get_op() << std::endl;
        std::string_view key_view;
        std::string_view value_view;
        key_view = itr.get_tuple().get_key();
        value_view = itr.get_tuple().get_value();
        std::cout << "key : " << key_view << std::endl;
        std::cout << "key_size : " << key_view.size() << std::endl;
        std::cout << "value : " << value_view << std::endl;
        std::cout << "value_size : " << value_view.size() << std::endl;
        std::cout << "----------" << std::endl;
        ++ctr;
    }
    std::cout << "==========" << std::endl;
}

Status session_info::check_delete_after_write(Storage storage, std::string_view key) { // NOLINT
    for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
        if (std::string_view{reinterpret_cast<char*>(&storage), sizeof(storage)} != itr->get_storage()) continue; // NOLINT

        // It can't use lange-based for because it use write_set.erase.
        std::string_view key_view = itr->get_rec_ptr()->get_tuple().get_key();
        if (key_view.size() == key.size() &&
            memcmp(key_view.data(), key.data(), key.size()) == 0) {
            if (itr->get_op() == OP_TYPE::INSERT) {
                Record* record = itr->get_rec_ptr();
                std::string_view key_view = record->get_tuple().get_key();
                yakushima::remove(get_yakushima_token(), itr->get_storage(), key_view);
                this->gc_handle_.get_rec_cont().push(itr->get_rec_ptr());

                /**
                 * create information for garbage collection.
                 */
                tid_word deletetid;
                deletetid.set_lock(false);
                deletetid.set_latest(false); // latest false mean that it asks checkpoint thread to remove from index.
                deletetid.set_absent(false);
                deletetid.set_epoch(this->get_epoch());
                storeRelease(record->get_tidw().obj_, deletetid.obj_); // NOLINT

                write_set.erase(itr);
                return Status::WARN_CANCEL_PREVIOUS_INSERT;
            }
            write_set.erase(itr);
            return Status::WARN_CANCEL_PREVIOUS_OPERATION;
        }
    }

    return Status::OK;
}

void session_info::remove_inserted_records_of_write_set_from_masstree() {
    for (auto&& itr : write_set) {
        if (itr.get_op() == OP_TYPE::INSERT) {
            Record* record = itr.get_rec_ptr();
            std::string_view key_view = record->get_tuple().get_key();
            yakushima::remove(get_yakushima_token(), itr.get_storage(), key_view);
            this->gc_handle_.get_rec_cont().push(itr.get_rec_ptr());

            /**
             * create information for garbage collection.
             */
            tid_word deletetid;
            deletetid.set_lock(false);
            deletetid.set_latest(false); // latest false mean that it asks checkpoint thread to remove from index.
            deletetid.set_absent(false);
            deletetid.set_epoch(this->get_epoch());
            storeRelease(record->get_tidw().obj_, deletetid.obj_); // NOLINT
        }
    }
}

write_set_obj* session_info::search_write_set(const Record* const rec_ptr) {
    for (auto&& itr : write_set) {
        if (itr.get_rec_ptr() == rec_ptr) return &itr;
    }
    return nullptr;
}

void session_info::unlock_write_set() {
    tid_word expected{};
    tid_word desired{};

    for (auto& itr : write_set) {
        if (itr.get_op() == OP_TYPE::INSERT) continue;
        // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
        Record* recptr = itr.get_rec_ptr();
        expected = loadAcquire(recptr->get_tidw().obj_); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(recptr->get_tidw().obj_, desired.obj_); // NOLINT
    }
}

void session_info::unlock_write_set( // NOLINT
        std::vector<write_set_obj>::iterator begin,
        std::vector<write_set_obj>::iterator end) {
    tid_word expected;
    tid_word desired;

    for (auto itr = begin; itr != end; ++itr) {
        if (itr->get_op() == OP_TYPE::INSERT) continue;
        // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
        expected = loadAcquire(itr->get_rec_ptr()->get_tidw().obj_); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(itr->get_rec_ptr()->get_tidw().obj_, desired.obj_); // NOLINT
    }
}

Status session_info::update_node_set(yakushima::node_version64* nvp) { // NOLINT
    for (auto&& elem : node_set) {
        if (std::get<1>(elem) == nvp) {
            yakushima::node_version64_body nvb = nvp->get_stable_version();
            if (std::get<0>(elem).get_vinsert_delete() + 1 != nvb.get_vinsert_delete()) {
                return Status::ERR_PHANTOM;
            }
            std::get<0>(elem) = nvb; // update
            // return Status::OK;
            /**
             * note : discussion.
             * Currently, node sets can have duplicate elements. If you allow duplicates, scanning will be easier.
             * Because scan doesn't have to do a match search, just add it to the end of node set. insert gets hard.
             * Even if you find a match, you have to search for everything because there may be other matches.
             * If you do not allow duplication, the situation is the opposite.
             */
        }
    }
    return Status::OK;
}

#ifdef PWAL

void session_info::pwal(uint64_t commit_id, commit_property cp) {
    for (auto&& itr : write_set) {
        if (itr.get_op() == OP_TYPE::UPDATE) {
            log_handle_.get_log_set().emplace_back(commit_id, itr.get_op(), &itr.get_tuple_to_local());
        } else {
            // insert/delete
            log_handle_.get_log_set().emplace_back(commit_id, itr.get_op(), &itr.get_tuple_to_db());
        }
        log_handle_.get_latest_log_header().add_checksum(
                log_handle_.get_log_set().back().compute_checksum()); // NOLINT
        log_handle_.get_latest_log_header().inc_log_rec_num();
    }

#if defined(PWAL_ENABLE_READ_LOG)
    for (auto&& itr : read_set) {
        log_handle_.get_log_set().emplace_back(commit_id, OP_TYPE::SEARCH, &itr.get_rec_read().get_tuple());
        log_handle_.get_latest_log_header().add_checksum(
                log_handle_.get_log_set().back().compute_checksum()); // NOLINT
        log_handle_.get_latest_log_header().inc_log_rec_num();
    }
#endif

    if (log_handle_.get_log_set().size() > PARAM_PWAL_LOG_GCOMMIT_THRESHOLD || cp == commit_property::WAIT_FOR_COMMIT) {
        // prepare write header
        log_handle_.get_latest_log_header().compute_two_complement_of_checksum();

        // write header
        log_handle_.get_log_file().write(static_cast<void*>(&log_handle_.get_latest_log_header()),
                                         sizeof(pwal::LogHeader));

        std::array<char, 4096> buffer{};
        std::size_t offset{0};

        auto write_batch = [this, &buffer, &offset](const void* ptr, size_t size) {
            if (size > 4096) { // NOLINT
                log_handle_.get_log_file().write(ptr, size);
                return;
            }
            if (offset + size > 4096) { // NOLINT
                // flush buffer.
                log_handle_.get_log_file().write(buffer.data(), offset);
                offset = 0;
            }
            memcpy(buffer.data() + offset, ptr, size);
            offset += size;
        };

        // write log record
        for (auto&& itr : log_handle_.get_log_set()) {
            // write tx id, op(operation type)
            write_batch(static_cast<void*>(&itr), sizeof(itr.get_tid()) + sizeof(itr.get_op()));

            // common subexpression elimination
            const Tuple* tupleptr = itr.get_tuple();

            std::string_view key_view = tupleptr->get_key();
            // write key_length
            // key_view.size() returns constexpr.
            std::size_t key_size = key_view.size();
            write_batch(static_cast<void*>(&key_size), sizeof(key_size));

            // write key_body
            write_batch(static_cast<const void*>(key_view.data()), key_size);

            std::string_view value_view = tupleptr->get_value();
            // write value_length
            // value_view.size() returns constexpr.
            std::size_t value_size = value_view.size();
            write_batch(static_cast<const void*>(value_view.data()), value_size);

            // write val_body
            if (itr.get_op() != OP_TYPE::DELETE) {
                if (value_size != 0) {
                    write_batch(static_cast<const void*>(value_view.data()), value_size); // NOLINT
                }
            }

            if (itr.get_tid() > this->get_flushed_ctid()) {
                this->set_flushed_ctid(itr.get_tid());
            }
        }
        // flush buffer
        log_handle_.get_log_file().write(buffer.data(), offset);
        offset = 0;
    }

    log_handle_.get_latest_log_header().init();
    log_handle_.get_log_set().clear();
}

#endif

#if defined(CPR)

void session_info::regi_diff_upd_set(std::string_view const storage, tid_word const tid, Record* const record, OP_TYPE const op_type) {
    auto& map{get_diff_upd_set()};
    map[std::string(storage)][std::string{record->get_tuple().get_key()}] = {tid, op_type != OP_TYPE::DELETE ? record : nullptr};
}

void session_info::regi_diff_upd_seq_set(SequenceValue id, std::tuple<SequenceVersion, SequenceValue> ver_val) {
    auto& map{get_diff_upd_seq_set()};
    map[id] = ver_val;
}

#endif

} // namespace shirakami
