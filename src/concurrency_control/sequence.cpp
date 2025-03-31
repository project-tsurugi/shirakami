/**
 * @file sequence.cpp
 */

#include <tuple>

#include "sequence.h"
#include "storage.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "database/include/logging.h"

namespace shirakami {

// public api

Status create_sequence(SequenceId* const id) {
    shirakami_log_entry << "create_sequence, id: " << id;
    auto ret = sequence::create_sequence(id);
    shirakami_log_exit << "create_sequence";
    return ret;
}

Status update_sequence(Token const token, SequenceId const id, // NOLINT
                       SequenceVersion const version,
                       SequenceValue const value) {
    shirakami_log_entry << "update_sequence, token: " << token << ", id: " << id
                        << ", version: " << version << ", value: " << value;
    auto ret = sequence::update_sequence(token, id, version, value);
    shirakami_log_exit << "update_sequence, " << ret;
    return ret;
}


Status read_sequence(SequenceId const id, SequenceVersion* const version,
                     SequenceValue* const value) {
    shirakami_log_entry << "read_sequence, id: " << id
                        << ", version: " << version << ", value: " << value;
    auto ret = sequence::read_sequence(id, version, value);
    shirakami_log_exit << "read_sequence, " << ret;
    return ret;
}

Status delete_sequence(SequenceId const id) {
    shirakami_log_entry << "delete_sequence, id: " << id;
    auto ret = sequence::delete_sequence(id);
    shirakami_log_exit << "delete_sequence, " << ret;
    return ret;
}

// sequence function body

void sequence::init() {
    sequence_map().clear();
    id_generator_ctr().store(1, std::memory_order_release);
}

Status sequence::generate_sequence_id(SequenceId& id) {
    id = id_generator_ctr().fetch_add(1);
    if (id == sequence::max_sequence_id) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "sequence id depletion";
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

void sequence::gc_sequence_map() {
    // compute gc epoch
    epoch::epoch_t gc_epoch{};
    gc_epoch = garbage::get_min_begin_epoch();
    if (gc_epoch > garbage::get_min_batch_epoch()) {
        gc_epoch = garbage::get_min_batch_epoch();
    }
    for (auto it = sequence::sequence_map().begin(); // NOLINT
         it != sequence::sequence_map().end();) {
        // avoid range based for-loop since erase() updates base map structure
        auto&& each_id_sequence_object = *it;
        auto&& each_sequence_object = each_id_sequence_object.second;
        if (each_sequence_object.rbegin()->first < gc_epoch &&
            each_sequence_object.rbegin()->second ==
                    sequence::non_exist_value) {
            // it was deleted and is able to gced.
            ++it;
            sequence::sequence_map().erase(each_id_sequence_object.first);
            continue;
        }
        std::size_t ctr{0};
        for (auto& itr : each_sequence_object) {
            if (itr.first < gc_epoch) {
                ++ctr;
            } else {
                break;
            }
        }
        if (ctr > 2) {
            // it can erase (ctr - 1) elements from begin.
            auto itr_begin = each_sequence_object.begin();
            auto itr_end = each_sequence_object.begin();
            advance(itr_end, ctr - 1);
            each_sequence_object.erase(itr_begin, itr_end);
        }
        ++it;
    }
}

// for delete_sequence
Status sequence::sequence_map_check_exist(SequenceId id) {
    // check the sequence object whose id is the arguments of this function.
    auto found_id_itr = sequence::sequence_map().find(id);
    if (found_id_itr == sequence::sequence_map().end()) {
        return Status::WARN_NOT_FOUND;
    } // found

    if (found_id_itr->second.empty()) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    } // not empty

    auto ritr = found_id_itr->second.rbegin();
    if (ritr->second == sequence::non_exist_value) {
        return Status::WARN_NOT_FOUND;
    }
    return Status::OK;
}

Status sequence::sequence_map_push(SequenceId const id) {
    std::tuple<SequenceVersion, SequenceValue> new_tuple{
            sequence::initial_value};
    return sequence::sequence_map_push(id, std::get<0>(new_tuple),
                                       std::get<1>(new_tuple));
}

Status sequence::sequence_map_push(SequenceId const id,
                                   SequenceVersion const version,
                                   SequenceValue const value) {
    return sequence::sequence_map_push(id, epoch::get_global_epoch(), version,
                                       value);
}

Status sequence::sequence_map_push(SequenceId const id,
                                   epoch::epoch_t const epoch,
                                   SequenceVersion const version,
                                   SequenceValue const value) {
    // push
    sequence::value_type new_val{};
    auto ret = sequence::sequence_map().emplace(id, new_val);
    if (ret.second) {
        // insert success
        auto ret2 = ret.first->second.emplace(
                epoch, std::make_tuple(version, value));
        if (!ret2.second) {
            // This object must be operated by here.
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix
                    << "When it tried to manipulate an object inserted into "
                       "map, it was already manipulated by someone else.";
            // maybe lack of mutex control
            return Status::ERR_FATAL;
        }
        return Status::OK;
    }
    return Status::WARN_ALREADY_EXISTS;
}

Status
sequence::sequence_map_find(SequenceId const id,
                            std::tuple<SequenceVersion, SequenceValue>& out) {
    // check the sequence object whose id is the arguments of this function.
    auto found_id_itr = sequence::sequence_map().find(id);
    if (found_id_itr == sequence::sequence_map().end()) {
        return Status::WARN_NOT_FOUND;
    } // found

    if (found_id_itr->second.empty()) {
        // it is empty
        return Status::WARN_NOT_FOUND;
    } // not empty

    auto last_itr = found_id_itr->second.rbegin();
    if (last_itr->second == sequence::non_exist_value) {
        // the sequence object was deleted.
        return Status::WARN_NOT_FOUND;
    }

    out = last_itr->second;
    return Status::OK;
}

// for update sequence function
Status sequence::sequence_map_find_and_verify(epoch::epoch_t epoch,
                                              SequenceId const id,
                                              SequenceVersion const version) {
    // check the sequence object whose id is the arguments of this function.
    auto found_id_itr = sequence::sequence_map().find(id);
    if (found_id_itr == sequence::sequence_map().end()) {
        return Status::WARN_NOT_FOUND;
    } // found

    if (found_id_itr->second.empty()) {
        // it is empty
        return Status::OK;
    } // not empty

    auto last_itr = found_id_itr->second.rbegin();
    if (last_itr->second == sequence::non_exist_value) {
        // the sequence object was deleted.
        return Status::WARN_NOT_FOUND;
    }

    if (std::get<0>(last_itr->second) < version) {
        if (last_itr->first > epoch) {
            // fix sequence map
            auto erased_itr = found_id_itr->second.end();
            erased_itr--;
            found_id_itr->second.erase(erased_itr);
        }
        return Status::OK;
    }
    return Status::WARN_ILLEGAL_OPERATION;
}

Status sequence::sequence_map_update(SequenceId const id,
                                     epoch::epoch_t const epoch,
                                     SequenceVersion const version,
                                     SequenceValue const value) {
    // check the sequence object whose id is the arguments of this function.
    auto found_id_itr = sequence::sequence_map().find(id);
    if (found_id_itr == sequence::sequence_map().end()) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    } // found

    // check the object belongs to the same epoch
    auto found_ob_itr = found_id_itr->second.find(epoch);
    std::tuple<SequenceVersion, SequenceValue> new_tuple;
    std::get<0>(new_tuple) = version;
    std::get<1>(new_tuple) = value;
    if (found_ob_itr == found_id_itr->second.end()) {
        // not found
        found_id_itr->second.emplace(epoch, new_tuple);
    } else {
        // found
        found_ob_itr->second = new_tuple;
    }

    return Status::OK;
}

Status sequence::create_sequence(SequenceId* id) {
    /**
     * acquire write lock.
     * Unless the updating and logging of the sequence map are combined into a
     * critical section, the timestamp ordering of updates and logging can be
     * confused with other concurrent operations.
     */
    std::lock_guard<std::shared_mutex> lk{sequence::sequence_map_smtx()};

    // gc after write lock
    sequence::gc_sequence_map();

    // generate sequence id
    auto ret = sequence::generate_sequence_id(*id);
    if (ret == Status::ERR_FATAL) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "sequence id depletion";
        return ret;
    }

    // generate sequence object
    ret = sequence::sequence_map_push(*id);
    if (ret == Status::WARN_ALREADY_EXISTS) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "Unexpected behavior. Id is already exist: " << *id;
        return Status::ERR_FATAL;
    }

    // generate transaction handle
    Token token{};
    while (Status::OK != enter(token)) { _mm_pause(); }

    // logging sequence operation
    // gen key
    std::string key{};
    key.append(reinterpret_cast<char*>(id), sizeof(*id)); // NOLINT
    // gen value
    std::tuple<SequenceVersion, SequenceValue> initial_pair =
            sequence::initial_value;
    SequenceVersion initial_version{std::get<0>(initial_pair)};
    SequenceValue initial_value{std::get<1>(initial_pair)};
    std::string value{}; // value is version + value
    value.append(reinterpret_cast<char*>(&initial_version), // NOLINT
                 sizeof(initial_version));
    value.append(reinterpret_cast<char*>(&initial_value), // NOLINT
                 sizeof(initial_value));
    ret = tx_begin({token, transaction_options::transaction_type::SHORT});
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "There is no way that short tx will fail to start here. "
                << ret;
    }
    ret = upsert(token, storage::sequence_storage, key, value);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "There is no way that upsert will fail to start here: "
                << ret;
        return Status::ERR_FATAL;
    }
    ret = commit(token);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "There is no way that 1 upsert only"
                                 "stx will fail here";
        return Status::ERR_FATAL;
    }

    // cleanup transaction handle
    ret = leave(token);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "There is no way that leave will fail here";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status sequence::update_sequence(Token const token, // NOLINT
                                 SequenceId const id,
                                 SequenceVersion const version,
                                 SequenceValue const value) {
    auto* ti = static_cast<session*>(token);

    // check whether it already began.
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    //ti->process_before_start_step();

    //ti->process_before_finish_step();
    return ti->sequence_set().push(id, version, value);
}

Status sequence::read_sequence(SequenceId const id,
                               SequenceVersion* const version,
                               SequenceValue* const value) {
    /**
     * acquire read lock.
     */
    std::shared_lock<std::shared_mutex> lk{sequence::sequence_map_smtx()};

    // read sequence object
    std::tuple<SequenceVersion, SequenceValue> out;
    auto ret = sequence::sequence_map_find(id, out);
    if (ret == Status::WARN_NOT_FOUND) { return ret; }
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    *version = std::get<0>(out);
    *value = std::get<1>(out);
    return Status::OK;
}

Status sequence::delete_sequence(SequenceId const id) {
    /**
     * acquire write lock.
     * Unless the updating and logging of the sequence map are combined into a
     * critical section, the timestamp ordering of updates and logging can be
     * confused with other concurrent operations.
     */
    std::lock_guard<std::shared_mutex> lk{sequence::sequence_map_smtx()};

    // gc after write lock
    sequence::gc_sequence_map();

    // check update sequence object
    auto ret = sequence::sequence_map_check_exist(id);
    if (ret == Status::WARN_NOT_FOUND) {
        // fail
        return ret;
    }
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    // generate transaction handle
    Token token{};
    while (Status::OK != enter(token)) { _mm_pause(); }

    // logging sequence operation
    // gen key
    std::string key{};
    key.append(reinterpret_cast<const char*>(&id), sizeof(id)); // NOLINT
    // gen value
    std::tuple<SequenceVersion, SequenceValue> new_tuple =
            sequence::non_exist_value;
    SequenceVersion version = std::get<0>(new_tuple);
    SequenceValue value = std::get<1>(new_tuple);
    std::string new_value{}; // value is version + value
    new_value.append(reinterpret_cast<const char*>(&version), // NOLINT
                     sizeof(version));
    new_value.append(reinterpret_cast<const char*>(&value), // NOLINT
                     sizeof(value));
    ret = tx_begin({token, transaction_options::transaction_type::SHORT});
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "there is no way stx begin will fail here. " << ret;
    }
    ret = upsert(token, storage::sequence_storage, key, new_value);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "there is no way stx's upsert will fail here";
        return Status::ERR_FATAL;
    }
    ret = commit(token);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "there is no way commit will fail here.";
        return Status::ERR_FATAL;
    }

    // update sequence object to deleted
    auto epoch = static_cast<session*>(token)->get_mrc_tid().get_epoch();
    ret = sequence::sequence_map_update(id, epoch, version, value);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix
                << "there is no way sequence map update will fail here.";
        return Status::ERR_FATAL;
    }

    // cleanup transaction handle
    ret = leave(token);
    if (ret != Status::OK) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix
                              << "there is no way leave will fail here.";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

} // namespace shirakami
