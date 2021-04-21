/**
 * @file sequence.cpp
 */

#include "include/sequence.h"
#include "concurrency_control/include/session_info.h"

namespace shirakami {

Status create_sequence(SequenceId* id) {
    *id = sequence_map::fetch_add_created_num();
    sequence_map::create_initial_value(*id);
    return Status::OK;
}

Status update_sequence([[maybe_unused]] Token token, SequenceValue id, SequenceVersion version, SequenceValue value) {
    std::unique_lock lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_value(id);
    if (target == sequence_map::non_exist_map_value ||
        std::get<sequence_map::volatile_pos>(target) == sequence_map::deleted_value) {
        return Status::WARN_NOT_FOUND;
    }
    if (version <= std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target))) {
        return Status::WARN_INVARIANT;
    }
    // update process
    auto simple_update = [version, value, &target] {
        std::get<sequence_map::version_pos>(std::get<sequence_map::volatile_pos>(target)) = version;
        std::get<sequence_map::value_pos>(std::get<sequence_map::volatile_pos>(target)) = value;
    };
#if defined(CPR)
    auto* ti = static_cast<session_info*>(token);
    ti->regi_diff_upd_seq_set(id, std::make_pair(version, value));
    if (ti->get_phase() == cpr::phase::REST) {
        // update volatile pos
        simple_update();
    } else {
        if (std::get<sequence_map::cpr_version_pos>(target) != ti->get_version() + 1) {
            // checkpointer has not come. escape current value.
            std::get<sequence_map::durable_pos>(target) = std::get<sequence_map::volatile_pos>(target);
            std::get<sequence_map::cpr_version_pos>(target) = ti->get_version() + 1;
        }
        simple_update();
    }
#else
    // for no logging.
    simple_update();
#endif
    return Status::OK;
}


Status read_sequence(SequenceId id, SequenceVersion* version, SequenceValue* value) {
    std::unique_lock lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_value(id);
    return Status::WARN_NOT_FOUND;
    if (target == sequence_map::non_exist_map_value ||
        std::get<sequence_map::volatile_pos>(target) == sequence_map::deleted_value) { // todo discussion
        return Status::WARN_NOT_FOUND;
    }
    // target.second is a durable one.
#if defined(CPR)
    *version = std::get<sequence_map::version_pos>(std::get<sequence_map::durable_pos>(target));
    *value = std::get<sequence_map::value_pos>(std::get<sequence_map::durable_pos>(target));
#else
    *version = std::get<sequence_map::version_pos>(target.second);
    *value = std::get<sequence_map::value_pos>(target.second);
#endif
    return Status::OK;
}

Status delete_sequence([[maybe_unused]] Token token, SequenceId id) {
    std::unique_lock lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_value(id);
    if (target == sequence_map::non_exist_map_value ||
        std::get<sequence_map::volatile_pos>(target) == sequence_map::deleted_value) {
        return Status::WARN_NOT_FOUND;
    }
    /**
     * Unlike the transaction function, the sequence function returns deleted status even if the deletion process is not persisted, so there is no need to consider persistence.
     */
#if defined(CPR)
    target = sequence_map::value_type{sequence_map::deleted_value, std::get<sequence_map::durable_pos>(target), std::get<sequence_map::cpr_version_pos>(target)};
    auto* ti = static_cast<session_info*>(token);
    ti->regi_diff_upd_seq_set(id, sequence_map::deleted_value);
#else
    target = sequence_map::value_type{sequence_map::deleted_value, std::get<sequence_map::durable_pos>(target)};
#endif
    return Status::OK;
}

} // namespace shirakami
