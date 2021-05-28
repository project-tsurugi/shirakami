/**
 * @file local_set.cpp
 */

#include "concurrency_control/include/local_set.h"
#include "concurrency_control/include/session_info.h"

namespace shirakami {

[[maybe_unused]] void local_write_set::display_write_set() {
    std::cout << "==========" << std::endl;
    std::cout << "start : session_info::display_write_set()" << std::endl;
    std::size_t ctr(1);
    auto display_we = [&ctr](write_set_obj* we_ptr) {
        std::cout << "Element #" << ctr << " of write set." << std::endl;
        std::cout << "rec_ptr_ : " << we_ptr->get_rec_ptr() << std::endl;
        std::cout << "op_ : " << we_ptr->get_op() << std::endl;
        std::string_view key_view;
        std::string_view value_view;
        key_view = we_ptr->get_tuple().get_key();
        value_view = we_ptr->get_tuple().get_value();
        std::cout << "key : " << key_view << std::endl;
        std::cout << "key_size : " << key_view.size() << std::endl;
        std::cout << "value : " << value_view << std::endl;
        std::cout << "value_size : " << value_view.size() << std::endl;
        std::cout << "----------" << std::endl;
        ++ctr;
    };
    if (get_for_batch()) {
        for (auto&& elem : get_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            display_we(we_ptr);
        }
    } else {
        for (auto&& elem : get_cont_for_ol()) {
            write_set_obj* we_ptr = &elem;
            display_we(we_ptr);
        }
    }
    std::cout << "==========" << std::endl;
}

write_set_obj* local_write_set::find(Record* rec_ptr) {
    // for bt
    if (for_batch_) {
        auto ret{cont_for_bt_.find(rec_ptr)};
        if (ret == cont_for_bt_.end()) {
            return nullptr;
        }
        return &std::get<1>(*ret);
    }
    // for ol
    for (auto&& elem : cont_for_ol_) {
        if (elem.get_rec_ptr() == rec_ptr) {
            return &elem;
        }
    }
    return nullptr;
}

local_write_set::cont_for_bt_type& local_write_set::get_cont_for_bt() {
    return cont_for_bt_;
}

local_write_set::cont_for_ol_type& local_write_set::get_cont_for_ol() {
    return cont_for_ol_;
}

void local_write_set::push(write_set_obj&& elem) {
    if (for_batch_) {
        cont_for_bt_.insert_or_assign(elem.get_rec_ptr(), std::move(elem));
    } else {
        cont_for_ol_.emplace_back(std::move(elem));
    }
}

void local_write_set::remove_inserted_records_from_yakushima(shirakami::Token shirakami_token, yakushima::Token yakushima_token) {
    auto process = [shirakami_token, yakushima_token](write_set_obj* we_ptr) {
        if (we_ptr->get_op() == OP_TYPE::INSERT) {
            Record* record = we_ptr->get_rec_ptr();
            std::string_view key_view = record->get_tuple().get_key();
            yakushima::remove(yakushima_token, we_ptr->get_storage(), key_view);
            auto* ti = static_cast<session_info*>(shirakami_token);
            ti->get_gc_handle().get_rec_cont().push(we_ptr->get_rec_ptr());

            /**
             * create information for garbage collection.
             */
            tid_word deletetid;
            deletetid.set_lock(false);
            deletetid.set_latest(false); // latest false mean that it asks checkpoint thread to remove from index.
            deletetid.set_absent(false);
            deletetid.set_epoch(ti->get_epoch());
            storeRelease(record->get_tidw().obj_, deletetid.obj_); // NOLINT
        }
    };

    if (get_for_batch()) {
        for (auto&& elem : get_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            process(we_ptr);
        }
    } else {
        for (auto&& elem : get_cont_for_ol()) {
            write_set_obj* we_ptr = &elem;
            process(we_ptr);
        }
    }
}

write_set_obj* local_write_set::search(const Record* const rec_ptr) {
    if (get_for_batch()) {
        for (auto&& elem : get_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (rec_ptr == we_ptr->get_rec_ptr()) {
                return we_ptr;
            }
        }
    } else {
        for (auto&& elem : get_cont_for_ol()) {
            write_set_obj* we_ptr = &elem;
            if (rec_ptr == we_ptr->get_rec_ptr()) {
                return we_ptr;
            }
        }
    }
    return nullptr;
}

void local_write_set::sort_if_ol() {
    if (for_batch_) return;
    std::sort(cont_for_ol_.begin(), cont_for_ol_.end());
}

void local_write_set::unlock() {
    auto process = [](Record* rec_ptr) {
        // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
        tid_word expected{};
        tid_word desired{};
        expected = loadAcquire(rec_ptr->get_tidw().obj_); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(rec_ptr->get_tidw().obj_, desired.obj_); // NOLINT
    };

    if (get_for_batch()) {
        for (auto&& elem : get_cont_for_bt()) {
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (we_ptr->get_op() == OP_TYPE::INSERT) continue;
            process(we_ptr->get_rec_ptr());
        }
    } else {
        for (auto&& elem : get_cont_for_ol()) {
            write_set_obj* we_ptr = &elem;
            if (we_ptr->get_op() == OP_TYPE::INSERT) continue;
            process(we_ptr->get_rec_ptr());
        }
    }
}

void local_write_set::unlock(std::size_t num) {
    auto process = [](write_set_obj* we_ptr) {
        tid_word expected{};
        tid_word desired{};
        expected = loadAcquire(we_ptr->get_rec_ptr()->get_tidw().obj_); // NOLINT
        desired = expected;
        desired.set_lock(false);
        storeRelease(we_ptr->get_rec_ptr()->get_tidw().obj_, desired.obj_); // NOLINT
    };
    std::size_t ctr{0};
    if (get_for_batch()) {
        auto& cont = get_cont_for_bt();
        for (auto&& elem : cont) {
            if (num == ctr) return;
            write_set_obj* we_ptr = &std::get<1>(elem);
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
                process(we_ptr);
            }
            ++ctr;
        }
    } else {
        auto& cont = get_cont_for_ol();
        for (auto&& elem : cont) {
            if (num == ctr) return;
            write_set_obj* we_ptr = &elem;
            if (we_ptr->get_op() != OP_TYPE::INSERT) {
                // inserted record's lock will be released at remove_inserted_records_of_write_set_from_masstree function.
                process(we_ptr);
            }
            ++ctr;
        }
    }
}

} // namespace shirakami