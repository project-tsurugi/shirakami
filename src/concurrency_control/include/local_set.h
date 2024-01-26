/**
 * @file concurrency_control/include/local_set.h
 */

#pragma once

#include <iterator>
#include <map>
#include <shared_mutex>
#include <sstream>
#include <string_view>

#include "cpu.h"
#include "record.h"
#include "sequence.h"

#include "yakushima/include/kvs.h"

#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

#include "glog/logging.h"

namespace shirakami {

class read_set_obj { // NOLINT
public:
    read_set_obj(Storage const storage, Record* const rec_ptr,
                 tid_word const tid) // NOLINT
        : storage_(storage), rec_ptr_(rec_ptr), tid_(tid) {}

    read_set_obj(const read_set_obj& right) = delete;
    read_set_obj(read_set_obj&& right) = default;

    read_set_obj& operator=(const read_set_obj& right) = delete; // NOLINT
    read_set_obj& operator=(read_set_obj&& right) = default;

    [[nodiscard]] Storage get_storage() const { return storage_; }

    [[nodiscard]] Record* get_rec_ptr() { return rec_ptr_; }

    [[nodiscard]] const Record* get_rec_ptr() const { return rec_ptr_; }

    [[nodiscard]] tid_word get_tid() const { return tid_; }

private:
    /**
     * @brief The target storage of this write.
     */
    Storage storage_{};

    /**
     * @brief Pointer to the read record in database.
     */
    Record* rec_ptr_{nullptr};

    /**
     * @brief Timestamp for optimistic read.
     */
    tid_word tid_{};
};

class write_set_obj { // NOLINT
public:
    // for update / upsert / insert
    write_set_obj(Storage const storage, OP_TYPE const op,
                  Record* const rec_ptr, std::string_view const val)
        : storage_(storage), op_(op), rec_ptr_(rec_ptr), val_(val) {
        if (op == OP_TYPE::DELETE) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        }
    }

    // for delete
    write_set_obj(Storage const storage, OP_TYPE const op,
                  Record* const rec_ptr)
        : storage_(storage), op_(op), rec_ptr_(rec_ptr) {
        if (op != OP_TYPE::DELETE) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        }
    }

    write_set_obj(const write_set_obj& right) = delete;
    write_set_obj(write_set_obj&& right) = default;

    write_set_obj& operator=(const write_set_obj& right) = delete;
    write_set_obj& operator=(write_set_obj&& right) = default;

    bool operator<(const write_set_obj& right) const {
        return get_rec_ptr() < right.get_rec_ptr();
    }

    bool operator==(const write_set_obj& right) const {
        return storage_ == right.storage_ && op_ == right.op_ &&
               rec_ptr_ == right.rec_ptr_ && val_ == right.val_;
    }

    void get_key(std::string& out) const { rec_ptr_->get_key(out); }

    [[nodiscard]] OP_TYPE get_op() const { return op_; }

    [[nodiscard]] Record* get_rec_ptr() const { return rec_ptr_; }

    [[nodiscard]] Storage get_storage() const { return storage_; }

    void get_value(std::string& out) const {
        if (get_op() == OP_TYPE::INSERT || get_op() == OP_TYPE::UPSERT ||
            get_op() == OP_TYPE::UPDATE) {
            out = val_;
            return;
        }
        if (get_op() == OP_TYPE::DELETE) { return; }
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
    }

    std::string_view get_value_view() { return val_; }

    void set_op(OP_TYPE op) { op_ = op; }

    void set_rec_ptr(Record* rec_ptr) { rec_ptr_ = rec_ptr; }

    /**
     * @brief set value
     * @details It is for twice update in the same transaction.
     */
    void set_val(std::string_view const val) { val_ = val; }

private:
    /**
     * @brief The target storage of this write.
     */
    Storage storage_{};
    /**
     * @brief The operation type of this write.
     */
    OP_TYPE op_{};
    /**
     * @brief Pointer to record.
     * @details For update, this is a pointer to existing record.
     * For insert, this is a pointer to new existing record.
     */
    Record* rec_ptr_{};
    /**
     * @brief Update cache for update.
     */
    std::string val_{}; // value for update
};

class local_write_set {
public:
    /**
     * @brief container type for short tx.
     */
    using cont_for_occ_type = std::vector<write_set_obj>;
    /**
     * @brief container type for batch (long tx).
     */
    using cont_for_bt_type = std::map<Record*, write_set_obj>;

    /**
     * @brief container for ltx info
    */
    using storage_map = std::map<Storage, std::tuple<std::string, std::string>>;

    /**
     * @brief clear containers.
     */
    void clear() {
        cont_for_occ_.clear();
        cont_for_bt_.clear();
        storage_map_.clear();
    }

    Status erase(write_set_obj* wso);

    [[nodiscard]] bool get_for_batch() const {
        return for_batch_.load(std::memory_order_acquire);
    }

    std::shared_mutex& get_mtx() { return mtx_; }

    cont_for_bt_type& get_ref_cont_for_bt() { return cont_for_bt_; }

    cont_for_occ_type& get_ref_cont_for_occ() { return cont_for_occ_; }

    /**
     * @brief get storage set
     * */
    storage_map& get_storage_map() { return storage_map_; }

    /**
     * @brief push an element to local write set.
     */
    void push(Token token, write_set_obj&& elem);

    /**
     * @brief check whether it already executed write operation.
     * @param[in] rec_ptr the target record.
     * @return the pointer of element. If it is nullptr, it is not found.
     */
    write_set_obj* search(Record const* rec_ptr);

    void set_for_batch(bool const tf) {
        for_batch_.store(tf, std::memory_order_release);
    }

    void sort_if_ol();

    /**
     * @brief unlock records in write set.
     *
     * This function unlocked all records in write set absolutely.
     * So it has a pre-condition.
     * @pre It has locked all records in write set.
     * @return void
     */
    void unlock();

    /**
     * @brief unlock write set object between @a begin and @a begin + num.
     * @param [in] num The number of removing.
     * @pre It already locked write set between @a begin and @a end.
     * @return void
     */
    void unlock(std::size_t num);

private:
    std::atomic<bool> for_batch_{false};
    /**
     * @brief container for batch.
     */
    cont_for_bt_type cont_for_bt_;
    /**
     * @brief container for short tx.
     */
    cont_for_occ_type cont_for_occ_;

    /**
     * @brief container for ltx.
     * @details This is not thread safe
    */
    storage_map storage_map_;

    std::shared_mutex mtx_;
};

inline std::ostream& operator<<(std::ostream& out,     // NOLINT
                                local_write_set& ws) { // NOLINT
                                                       // now occ only
    std::stringstream ss;
    ss << "for_batch_: " << ws.get_for_batch() << std::endl;
    // for occ container
    ss << "about occ container" << std::endl;
    for (auto itr = ws.get_ref_cont_for_occ().begin();
         itr != ws.get_ref_cont_for_occ().end(); ++itr) {
        ss << "No " << std::distance(ws.get_ref_cont_for_occ().begin(), itr)
           << ", storage_: " << itr->get_storage() << ", op_: " << itr->get_op()
           << ", rec_ptr_: " << itr->get_rec_ptr() << ", val_: ";
        std::string val_buf{};
        itr->get_value(val_buf);
        ss << val_buf << std::endl;
    }
    out << ss.str();
    return out;
}

class local_sequence_set {
public:
    using container_type =
            std::map<SequenceId, std::tuple<SequenceVersion, SequenceValue>>;

    void clear() { set_.clear(); }
    container_type& set() { return set_; }

    Status push(SequenceId id, SequenceVersion version, SequenceValue value);

private:
    container_type set_;
};

/**
 * @brief It is used for registering read information.
 * 
 */
class local_read_set_for_ltx {
public:
    using cont_type = std::set<Record*>;

    std::shared_mutex& get_mtx_set() { return mtx_set_; }

    void clear() {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        set_.clear();
    }

    void push(Record* rec) {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        set_.insert(rec);
    }

    // getter
    cont_type& set() { return set_; }

private:
    cont_type set_;

    std::shared_mutex mtx_set_{};
};

class node_set {
public:
    using set_type = std::vector<std::pair<yakushima::node_version64_body,
                                           yakushima::node_version64*>>;

    auto clear() {
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        return get_set().clear();
    }

    Status update_node_set(yakushima::node_version64* nvp) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        for (auto&& elem : set_) {
            if (std::get<1>(elem) == nvp) {
                yakushima::node_version64_body nvb = nvp->get_stable_version();
                if (std::get<0>(elem).get_vinsert_delete() + 1 !=
                    nvb.get_vinsert_delete()) {
                    return Status::ERR_CC;
                }
                std::get<0>(elem) = nvb; // update
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

    Status emplace_back(std::pair<yakushima::node_version64_body,
                                  yakushima::node_version64*>
                                elem) {
        // take write lock
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        // engineering optimization, shrink nvec size.
        if (!get_set().empty() &&       // not empty
            get_set().back() == elem) { // last elem is same
            return Status::OK;          // skip registering.
        }

        // early validation
        auto cnvp = std::get<1>(elem)->get_stable_version();
        if (cnvp != std::get<0>(elem)) {
            // looks like phantom
            // check self phantom possibility
            for (auto&& elem_set : set_) {
                if (std::get<1>(elem_set) == std::get<1>(elem)) {
                    /**
                      * Node versions already added in a previous scan operation.
                      */
                    if (cnvp == std::get<0>(elem_set)) {
                        // the difference due to old self insert.
                        return Status::OK;
                    }
                    // the difference include other tx's insert
                    return Status::ERR_CC;
                }
            }
            // phantom due to other tx's insert
            return Status::ERR_CC;
        }

        get_set().emplace_back(elem);
        return Status::OK;
    }

    auto empty() {
        std::shared_lock<std::shared_mutex> lk{get_mtx_set()};
        return get_set().empty();
    }

    auto front() {
        std::shared_lock<std::shared_mutex> lk{get_mtx_set()};
        return get_set().front();
    }

    std::shared_mutex& get_mtx_set() { return mtx_set_; }

    set_type& get_set() { return set_; }

    Status node_verify() {
        std::shared_lock<std::shared_mutex> lk{get_mtx_set()};
        for (auto&& itr : get_set()) {
            auto old_id = std::get<0>(itr);
            auto current_id = std::get<1>(itr)->get_stable_version();
            if (old_id.get_vinsert_delete() !=
                        current_id.get_vinsert_delete() ||
                old_id.get_vsplit() != current_id.get_vsplit()) {
                return Status::ERR_CC;
            }
        }
        return Status::OK;
    }

    auto size() {
        std::shared_lock<std::shared_mutex> lk{get_mtx_set()};
        return get_set().size();
    }

private:
    /**
     * @brief local set for phantom avoidance.
     */
    set_type set_;

    /**
     * @brief mutex for local node set
    */
    std::shared_mutex mtx_set_;
};

class range_read_set_for_ltx {
public:
    using elem_type = std::tuple<range_read_by_long*, std::string,
                                 scan_endpoint, std::string, scan_endpoint>;
    using set_type = std::set<elem_type>;

    std::shared_mutex& get_mtx_set() { return mtx_set_; }

    set_type& get_set() { return set_; }

    void clear() {
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        set_.clear();
    }

    void insert(elem_type const& elem) {
        std::lock_guard<std::shared_mutex> lk{get_mtx_set()};
        set_.insert(elem);
    }

private:
    set_type set_;

    std::shared_mutex mtx_set_{};
};

} // namespace shirakami