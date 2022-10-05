/**
 * @file sequence.cpp
 */

#include "sequence.h"

namespace shirakami {

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

Status sequence::create_sequence([[maybe_unused]] SequenceId* id) {
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