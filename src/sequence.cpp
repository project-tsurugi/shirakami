/**
 * @file sequence.cpp
 */

#include "include/sequence.h"
#include "concurrency_control/include/session_info.h"

namespace shirakami {

Status create_sequence(SequenceId* id) {
    *id = sequence_map::fetch_add_created_num();
    sequence_map::create_initial_value(*id);

#if defined(CPR)
    Token token{};
    while (enter(token) != Status::OK) {
        ;
    }
    tx_begin(token);
    auto* ti = static_cast<session_info*>(token);
    ti->regi_diff_upd_seq_set(*id, sequence_map::initial_value);
    commit(token);
    leave(token);
#endif
    return Status::OK;
}

Status update_sequence([[maybe_unused]] Token token, SequenceId id, SequenceVersion version, SequenceValue value) {
    std::unique_lock lock{sequence_map::get_sm_mtx()};
    sequence_map::value_type target{};
    if (sequence_map::get_value(id, target) == Status::WARN_NOT_FOUND) return Status::WARN_NOT_FOUND;

    // check existence
    if (
#if defined(CPR)
            std::get<sequence_map::volatile_pos>(target) == sequence_map::non_exist_value
#else
            target == sequence_map::non_exist_value
#endif
    ) {
        return Status::WARN_NOT_FOUND;
    }

    // check invariant
#if defined(CPR)
    if (version <= std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target))) {
#else
    if (version <= std::get<sequence_map::version_pos>(target)) {
#endif
        return Status::WARN_INVARIANT;
    }

    // get reference
    sequence_map::value_type& target_ref = sequence_map::get_value_ref(id);

    // update process
    auto simple_update = [version, value, &target_ref] {
#if defined(CPR)
        std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target_ref)) = version;
        std::get<sequence_map::value_pos>(std::get<sequence_map::volatile_pos>(target_ref)) = value;
#else
        std::get<sequence_map::version_pos>(target_ref) = version;
        std::get<sequence_map::value_pos>(target_ref) = value;
#endif
    };

#if defined(CPR)
    // register diff hint
    auto* ti = static_cast<session_info*>(token);
    ti->regi_diff_upd_seq_set(id, std::make_pair(version, value));
    if (ti->get_phase() != cpr::phase::REST) {
        if (std::get<sequence_map::cpr_version_pos>(target) != ti->get_version() + 1) {
            // checkpointer has not come. escape current value.
            std::get<sequence_map::durable_pos>(target_ref) = std::get<sequence_map::volatile_pos>(target_ref);
            std::get<sequence_map::cpr_version_pos>(target_ref) = ti->get_version() + 1;
        }
    }
#endif

    simple_update();
    return Status::OK;
}


Status read_sequence(SequenceId id, SequenceVersion* version, SequenceValue* value) {
    std::unique_lock lock{sequence_map::get_sm_mtx()};
    sequence_map::value_type target{};
    if (sequence_map::get_value(id, target) == Status::WARN_NOT_FOUND) {
        return Status::WARN_NOT_FOUND;
    }

    // check existence
    if (
#if defined(CPR)
            std::get<sequence_map::volatile_pos>(target) == sequence_map::non_exist_value
#else
            target == sequence_map::non_exist_value // if deleted value exist.
#endif
    ) {
        return Status::WARN_NOT_FOUND;
    }

    // target.second is a durable one.
#if defined(CPR)
    Token token{};
    while (enter(token) != Status::OK) {
        ;
    }
    tx_begin(token);
    auto* ti = static_cast<session_info*>(token);
    // get reference
    sequence_map::value_type& target_ref = sequence_map::get_value_ref(id);
    if (ti->get_phase() == cpr::phase::REST) {
        if (std::get<sequence_map::cpr_version_pos>(target) < ti->get_version()) {
            // read from volatile and update for durable
            *version = std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target));
            *value = std::get<sequence_map::value_pos>(std::get<sequence_map::volatile_pos>(target));
            std::get<sequence_map::durable_pos>(target_ref) = std::get<sequence_map::volatile_pos>(target_ref);
            std::get<sequence_map::cpr_version_pos>(target_ref) = ti->get_version();
        } else {
            // read from durable
            *version = std::get<sequence_map::version_pos>(std::get<sequence_map::durable_pos>(target));
            *value = std::get<sequence_map::value_pos>(std::get<sequence_map::durable_pos>(target));
        }
    } else {
        // after logical boundary of cpr.
        if (std::get<sequence_map::cpr_version_pos>(target) < ti->get_version() + 1) {
            // checkpointer does not come yet.
            if (std::get<sequence_map::cpr_version_pos>(target) < ti->get_version()) {
                // read from volatile
                *version = std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target));
                *value = std::get<sequence_map::value_pos>(std::get<sequence_map::volatile_pos>(target));
            } else {
                // read from durable
                *version = std::get<sequence_map::version_pos>(std::get<sequence_map::durable_pos>(target));
                *value = std::get<sequence_map::value_pos>(std::get<sequence_map::durable_pos>(target));
            }
            // update durable for checkpointer.
            std::get<sequence_map::durable_pos>(target_ref) = std::get<sequence_map::volatile_pos>(target_ref);
            std::get<sequence_map::cpr_version_pos>(target_ref) = ti->get_version() + 1;
        } else {
            // read from durable
            *version = std::get<sequence_map::version_pos>(std::get<sequence_map::durable_pos>(target));
            *value = std::get<sequence_map::value_pos>(std::get<sequence_map::durable_pos>(target));
        }
    }
    commit(token);
    leave(token);
#else
    *version = std::get<sequence_map::version_pos>(target);
    *value = std::get<sequence_map::value_pos>(target);
#endif
    return Status::OK;
}

Status delete_sequence(SequenceId id) {
    std::unique_lock lock{sequence_map::get_sm_mtx()};
    sequence_map::value_type& target = sequence_map::get_value_ref(id);

    // check existence
    if (
#if defined(CPR)
            std::get<sequence_map::volatile_pos>(target) == sequence_map::non_exist_value
#else
            target == sequence_map::non_exist_value
#endif
    ) {
        return Status::WARN_NOT_FOUND;
    }

    /**
     * Unlike the transaction function, the sequence function returns deleted status even if the deletion process is not persisted, so there is no need to consider persistence.
     */
#if defined(CPR)
    Token token{};
    while (enter(token) != Status::OK) {
        ;
    }
    tx_begin(token); // for cordinate with cpr.

    target = sequence_map::value_type{sequence_map::non_exist_value, std::get<sequence_map::durable_pos>(target), std::get<sequence_map::cpr_version_pos>(target)};
    auto* ti = static_cast<session_info*>(token);
    ti->regi_diff_upd_seq_set(id, sequence_map::non_exist_value);
    commit(token);
    leave(token);
#else
    target = sequence_map::value_type{sequence_map::non_exist_value};
#endif

    return Status::OK;
}

} // namespace shirakami
