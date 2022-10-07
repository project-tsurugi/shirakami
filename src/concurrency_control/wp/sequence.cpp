/**
 * @file sequence.cpp
 */

#include "sequence.h"

#include "concurrency_control/wp/include/garbage.h"

namespace shirakami {

// public api

Status create_sequence(SequenceId* id) { return sequence::create_sequence(id); }

Status update_sequence(Token token, SequenceId id, SequenceVersion version,
                       SequenceValue value) {
    return sequence::update_sequence(token, id, version, value);
}


Status read_sequence(SequenceId id, SequenceVersion* version,
                     SequenceValue* value) {
    return sequence::read_sequence(id, version, value);
}

Status delete_sequence(SequenceId id) { return sequence::delete_sequence(id); }

// sequence function body

void sequence::init() {
    sequence_map().clear();
    id_generator_ctr().store(0, std::memory_order_release);
}

Status sequence::generate_sequence_id(SequenceId& id) {
    id = id_generator_ctr().fetch_add(1);
    if (id == sequence::max_sequence_id) {
        // sequence id depletion.
        return Status::ERR_FATAL;
    }
    return Status::OK;
}

Status sequence::sequence_map_push([[maybe_unused]] SequenceId const id) {
#if 0
    std::lock_guard<std::shared_mutex> lk{sequence::sequence_map_smtx()};
    // gc
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
            if (itr.first < gc_epoch) {
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

    // push
    sequence::value_type new_val{};
    auto ret = sequence::sequence_map().insert(std::make_pair(id, new_val));
    if (ret.second) {
        ret = (*ret.first)
                      .insert(std::make_pair(epoch::get_global_epoch(),
                                             sequence::initial_value));
        if (!ret.second) {
            // This object must be operated by here.
            LOG(ERROR) << "unexpected behavior";
            return Status::ERR_FATAL;
        }
        return Status::OK;
    }
    return Status::WARN_ALREADY_EXISTS;
#else
    return Status::ERR_NOT_IMPLEMENTED;
#endif
}

Status sequence::create_sequence(SequenceId* id) {
    // generate sequence id
    auto ret = sequence::generate_sequence_id(*id);
    if (ret == Status::ERR_FATAL) { return ret; }

    // generate sequence object
    ret = sequence::sequence_map_push(*id);
    if (ret == Status::WARN_ALREADY_EXISTS) {
        LOG(ERROR) << "unexpected behavior";
        return Status::ERR_FATAL;
    }

#if 0
    // generate transaction handle
    Token token{};
    while (Status::OK != enter(token)) { _mm_pause(); }

    // logging sequence operation
    std::string key{};
    key.append(reinterpret_cast<char*>(*id), sizeof(*id));
    std::tuple<SequenceVersion, SequenceValue> inital_value =
            sequence::initial_value;
    std::string value{};
    std::value.append(reinterpret_cast<char*>()) ret =
            upsert(token, storage::sequence_storage, );

#endif
    // cleanup transaction handle
    // copy sequence id to output arguments
    return Status::ERR_NOT_IMPLEMENTED;
}

Status sequence::update_sequence([[maybe_unused]] Token token,
                                 [[maybe_unused]] SequenceId id,
                                 [[maybe_unused]] SequenceVersion version,
                                 [[maybe_unused]] SequenceValue value) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status sequence::read_sequence([[maybe_unused]] SequenceId id,
                               [[maybe_unused]] SequenceVersion* version,
                               [[maybe_unused]] SequenceValue* value) {
    return Status::ERR_NOT_IMPLEMENTED;
}

Status sequence::delete_sequence([[maybe_unused]] SequenceId id) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami