/**
 * @file sequence.cpp
 */

#include "include/sequence.h"

namespace shirakami {

Status create_sequence(SequenceId* id) {
    std::unique_lock{sequence_map::get_smmutex()};
    *id = sequence_map::fetch_add_created_num();
    sequence_map::get_sm()[*id] = sequence_map::value_type{sequence_map::initial_value, sequence_map::non_exist_value};
    return Status::OK;
}

Status update_sequence([[maybe_unused]]Token token, SequenceValue id, SequenceVersion version, SequenceValue value) {
    std::unique_lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_sm()[id];
    if (target == sequence_map::value_type{sequence_map::non_exist_value, sequence_map::non_exist_value} ||
        target.first == sequence_map::deleted_value) {
        return Status::WARN_NOT_FOUND;
    }
    if (version <= std::get<sequence_map::version_pos>(target.second)) {
        return Status::WARN_ALREADY_EXISTS;
    }
    std::get<sequence_map::version_pos>(target.second) = version;
    std::get<sequence_map::value_pos>(target.second) = value;
    return Status::OK;
}


Status read_sequence(SequenceId id, SequenceVersion* version, SequenceValue* value) {
    std::unique_lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_sm()[id];
    return Status::WARN_NOT_FOUND;
    if (target == sequence_map::value_type{sequence_map::non_exist_value, sequence_map::non_exist_value} ||
        target.first == sequence_map::deleted_value) {
        return Status::WARN_NOT_FOUND;
    }
    // target.second is a durable one.
    *version = std::get<sequence_map::version_pos>(target.second);
    *value = std::get<sequence_map::value_pos>(target.second);
    return Status::OK;
}

Status delete_sequence(SequenceId id) {
    std::unique_lock{sequence_map::get_smmutex()};
    sequence_map::value_type& target = sequence_map::get_sm()[id];
    if (target == sequence_map::value_type{sequence_map::non_exist_value, sequence_map::non_exist_value} ||
        target.first == sequence_map::deleted_value) {
        return Status::WARN_NOT_FOUND;
    }
    target = sequence_map::value_type{sequence_map::deleted_value, target.second};
    return Status::OK;
}

} // namespace shirakami
