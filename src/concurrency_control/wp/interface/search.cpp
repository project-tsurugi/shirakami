

#include <string_view>

#include "concurrency_control/wp/include/helper.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status search_key(Token const token, Storage const storage,
                  std::string_view const key, Tuple** const tuple) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) {
        tx_begin(token); // NOLINT
    }
    if (ti->get_mode() == tx_mode::BATCH &&
        epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }

    // index access
    Record** rec_d_ptr{std::get<0>(
            yakushima::get<Record*>({reinterpret_cast<const char*>(&storage),
                                     sizeof(storage)}, // NOLINT
                                    key))};
    if (rec_d_ptr == nullptr) {
        *tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_d_ptr};

    // check local write set
    write_set_obj* in_ws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (in_ws != nullptr) {
        if (in_ws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        ti->get_cache_for_search_ptr()->get_pimpl()->set_key(
                in_ws->get_rec_ptr()->get_key());
        ti->get_cache_for_search_ptr()->get_pimpl()->set_val(in_ws->get_val());
        *tuple = ti->get_cache_for_search_ptr();
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    if (ti->get_mode() == tx_mode::OCC) {
        // occ
        // wp check
        wp::wp_meta::wped_type wps{std::move(find_wp(storage))};
        if (!wps.empty()) {
            abort(token);
            return Status::ERR_VALIDATION;
        }
        tid_word read_tid;
        std::string* read_val;
        // read version
        Status rs{read_record(rec_ptr, read_tid, read_val)};
        if (rs == Status::OK) {
            ti->get_read_set().emplace_back(storage, rec_ptr, read_tid,
                                            read_val);
            ti->get_cache_for_search_ptr()->get_pimpl()->set_key(key);
            ti->get_cache_for_search_ptr()->get_pimpl()->set_val(*read_val);
            *tuple = ti->get_cache_for_search_ptr();
        } else {
            *tuple = nullptr;
        }
        return rs;
    } else {
        // batch
        // version selection
        version* ver{};
        tid_word f_check{loadAcquire(&rec_ptr->get_tidw_ref().get_obj())};
        for (;;) {
            ver = rec_ptr->get_latest();
            tid_word s_check{loadAcquire(&rec_ptr->get_tidw_ref().get_obj())};
            if (f_check == s_check) {
                // Whatever value tid is, ver is the latest version.
                break;
            } else {
                _mm_pause();
                f_check = s_check;
            }
        }
        auto valid_version_tuple_register = [&ti, &rec_ptr, &ver, &tuple]() {
            ti->get_cache_for_search_ptr()->get_pimpl()->set_key(
                    rec_ptr->get_key());
            ti->get_cache_for_search_ptr()->get_pimpl()->set_val(
                    *ver->get_value());
            *tuple = ti->get_cache_for_search_ptr();
        };
        for (;;) {
            if (ver == nullptr) {
                LOG(FATAL) << "unreachable";
                std::abort();
            }
            if (ti->get_valid_epoch() > f_check.get_epoch()) {
                valid_version_tuple_register();
                return Status::OK;
            } else {
                ver = ver->get_next();
            }
        }
    }
}

} // namespace shirakami
