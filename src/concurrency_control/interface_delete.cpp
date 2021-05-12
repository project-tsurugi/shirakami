/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"
#include "storage.h"

#include "concurrency_control/include/garbage_collection.h"
#include "concurrency_control/include/interface_helper.h"

#include "shirakami/interface.h"

// sizeof(Tuple)

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    std::vector<Storage> storage_list;
    list_storage(storage_list);
    for (auto&& elem : storage_list) {
        std::vector<std::tuple<std::string, Record**, std::size_t>> scan_res;
        constexpr std::size_t v_index{1};
        yakushima::scan({reinterpret_cast<char*>(&elem), sizeof(elem)}, "", yakushima::scan_endpoint::INF, "", yakushima::scan_endpoint::INF, scan_res); // NOLINT

        if (scan_res.size() < std::thread::hardware_concurrency() * 10) { // NOLINT
                                                                          // single thread clean up
            for (auto&& itr : scan_res) {
                delete *std::get<v_index>(itr); // NOLINT
            }
        } else {
            // multi threads clean up
            auto process = [&scan_res]([[maybe_unused]] std::size_t const begin, [[maybe_unused]] std::size_t const end) {
                for (std::size_t i = begin; i < end; ++i) {
                    delete *std::get<v_index>(scan_res[i]); // NOLINT
                }
            };
            std::size_t th_size = std::thread::hardware_concurrency();
            std::vector<std::thread> th_vc;
            th_vc.reserve(th_size);
            for (std::size_t i = 0; i < th_size; ++i) {
                th_vc.emplace_back(process, i * (scan_res.size() / th_size), i != th_size - 1 ? (i + 1) * (scan_res.size() / th_size) : scan_res.size());
            }
            for (auto&& th : th_vc) th.join();
        }
    }

    yakushima::destroy();

    return Status::OK;
}

Status delete_record(Token token, Storage storage, const std::string_view key) { // NOLINT
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    if (ti->get_read_only()) return Status::WARN_INVALID_HANDLE;
    Status check = ti->check_delete_after_write(key);

    Record** rec_double_ptr{yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key).first}; // NOLINT
    if (rec_double_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_double_ptr};
    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        // The second condition checks
        // whether the record you want to read should not be read by parallel
        // insert / delete.
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().emplace_back(storage, OP_TYPE::DELETE, rec_ptr);
    return check;
}

} // namespace shirakami
