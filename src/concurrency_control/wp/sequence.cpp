/**
 * @file sequence.cpp
 */

#include <tuple>

#include "sequence.h"
#include "storage.h"

#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

namespace shirakami {

// public api

Status create_sequence(SequenceId* const id) {
    return sequence::create_sequence(id);
}

Status update_sequence(Token const token, SequenceId const id,
                       SequenceVersion const version,
                       SequenceValue const value) {
    return sequence::update_sequence(token, id, version, value);
}


Status read_sequence(SequenceId const id, SequenceVersion* const version,
                     SequenceValue* const value) {
    return sequence::read_sequence(id, version, value);
}

Status delete_sequence([[maybe_unused]] SequenceId id) { return Status::OK; }

// sequence function body

void sequence::init() {
    sequence_map().clear();
    id_generator_ctr().store(0, std::memory_order_release);
}

Status sequence::generate_sequence_id(SequenceId& id) {
    id = id_generator_ctr().fetch_add(1);
    if (id == sequence::max_sequence_id) {
        LOG(ERROR) << "sequence id depletion";
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

void sequence::gc_sequence_map() {
    // compute gc epoch
    epoch::epoch_t gc_epoch{};
    gc_epoch = garbage::get_min_step_epoch();
    if (gc_epoch > garbage::get_min_batch_epoch()) {
        gc_epoch = garbage::get_min_batch_epoch();
    }
    for (auto&& each_id_sequence_object : sequence::sequence_map()) {
        auto&& each_sequence_object = each_id_sequence_object.second;
        std::size_t ctr{0};
        for (auto itr = each_sequence_object.begin();
             itr != each_sequence_object.end();) {
            if (itr->first < gc_epoch) {
                ++ctr;
            } else {
                break;
            }
        }
        if (ctr > 2) {
            // it can erase (ctr - 1) times.
            for (std::size_t i = 0; i < ctr - 1; ++i) {
                sequence::sequence_map().erase(
                        sequence::sequence_map().begin());
            }
        }
    }
}

Status sequence::sequence_map_push(SequenceId const id) {
    // gc
    sequence::gc_sequence_map();

    // push
    sequence::value_type new_val{};
    auto ret = sequence::sequence_map().insert(std::make_pair(id, new_val));
    if (ret.second) {
        // insert success
        auto ret2 = ret.first->second.insert(std::make_pair(
                epoch::get_global_epoch(), sequence::initial_value));
        if (!ret2.second) {
            // This object must be operated by here.
            LOG(ERROR) << "unexpected behavior";
            return Status::ERR_FATAL;
        }
        return Status::OK;
    }
    return Status::WARN_ALREADY_EXISTS;
}

Status
sequence::sequence_map_find(SequenceId id, epoch::epoch_t epoch,
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

    for (auto ritr = found_id_itr->second.rbegin();
         ritr != found_id_itr->second.rend(); ++ritr) {
        if (ritr->first <= epoch) {
            out = ritr->second;
            return Status::OK;
        }
    }
    return Status::WARN_NOT_FOUND;
}

Status sequence::sequence_map_find_and_verify(SequenceId const id,
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

    auto last_itr = found_id_itr->second.end();
    last_itr--;
    if (std::get<0>(last_itr->second) < version) { return Status::OK; }
    return Status::WARN_ILLEGAL_OPERATION;
}

Status sequence::sequence_map_update(SequenceId const id,
                                     epoch::epoch_t const epoch,
                                     SequenceVersion const version,
                                     SequenceValue const value) {
    // gc
    sequence::gc_sequence_map();

    // check the sequence object whose id is the arguments of this function.
    auto found_id_itr = sequence::sequence_map().find(id);
    if (found_id_itr == sequence::sequence_map().end()) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    } // found

    // check the object belongs to the same epoch
    auto found_ob_itr = found_id_itr->second.find(epoch);
    std::tuple<SequenceVersion, SequenceValue> new_tuple;
    std::get<0>(new_tuple) = version;
    std::get<1>(new_tuple) = value;
    if (found_ob_itr == found_id_itr->second.end()) {
        // not found
        found_id_itr->second.insert(std::make_pair(epoch, new_tuple));
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

    // generate sequence id
    auto ret = sequence::generate_sequence_id(*id);
    if (ret == Status::ERR_FATAL) { return ret; }

    // generate sequence object
    ret = sequence::sequence_map_push(*id);
    if (ret == Status::WARN_ALREADY_EXISTS) {
        LOG(ERROR) << "unexpected behavior";
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
    ret = upsert(token, storage::sequence_storage, key, value);
    if (ret != Status::OK) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }
    ret = commit(token);
    if (ret != Status::OK) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }

    // cleanup transaction handle
    ret = leave(token);
    if (ret != Status::OK) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status sequence::update_sequence(Token const token, SequenceId const id,
                                 SequenceVersion const version,
                                 SequenceValue const value) {
    /**
     * acquire write lock.
     * Unless the updating and logging of the sequence map are combined into a 
     * critical section, the timestamp ordering of updates and logging can be 
     * confused with other concurrent operations.
     */
    std::lock_guard<std::shared_mutex> lk{sequence::sequence_map_smtx()};

    // check update sequence object
    auto ret = sequence::sequence_map_find_and_verify(id, version);
    if (ret == Status::WARN_ILLEGAL_OPERATION ||
        ret == Status::WARN_NOT_FOUND) {
        // fail
        return ret;
    } else if (ret != Status::OK) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    // logging sequence operation
    // gen key
    std::string key{};
    key.append(reinterpret_cast<const char*>(&id), sizeof(id)); // NOLINT
    // gen value
    std::string new_value{}; // value is version + value
    new_value.append(reinterpret_cast<const char*>(&version), // NOLINT
                     sizeof(version));
    new_value.append(reinterpret_cast<const char*>(&value), // NOLINT
                     sizeof(value));
    ret = upsert(token, storage::sequence_storage, key, new_value);
    if (ret != Status::OK) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }
    ret = commit(token);
    if (ret != Status::OK) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }

    // update sequence object
    auto epoch = static_cast<session*>(token)->get_mrc_tid().get_epoch();
    ret = sequence::sequence_map_update(id, epoch, version, value);
    if (ret != Status::OK) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    return Status::OK;
}

Status sequence::read_sequence(SequenceId const id,
                               SequenceVersion* const version,
                               SequenceValue* const value) {
    /**
     * acquire read lock.
     */
    std::shared_lock<std::shared_mutex> lk{sequence::sequence_map_smtx()};

    // get durable epoch
    auto epoch = lpwal::get_durable_epoch();

    // read sequence object
    std::tuple<SequenceVersion, SequenceValue> out;
    auto ret = sequence::sequence_map_find(id, epoch, out);
    if (ret == Status::WARN_NOT_FOUND) { return ret; }
    if (ret != Status::OK) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    *version = std::get<0>(out);
    *value = std::get<1>(out);
    return Status::OK;
}

Status sequence::delete_sequence([[maybe_unused]] SequenceId id) {
    // update sequence object
    // generate transaction handle
    // logging sequence operation
    // gen key
    // gen value
    // cleanup transaction handle
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami