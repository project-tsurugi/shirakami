/**
 * @file sequence.cpp
 */

#include "include/sequence.h"

namespace shirakami {

Status create_sequence([[maybe_unused]] SequenceId* id,
                       [[maybe_unused]] Token token) {
    return Status::OK;
}

Status update_sequence([[maybe_unused]] Token token,
                       [[maybe_unused]] SequenceId id,
                       [[maybe_unused]] SequenceVersion version,
                       [[maybe_unused]] SequenceValue value) {
    return Status::OK;
}


Status read_sequence([[maybe_unused]] SequenceId id,
                     [[maybe_unused]] SequenceVersion* version,
                     [[maybe_unused]] SequenceValue* value,
                     [[maybe_unused]] Token token) {
    return Status::OK;
}

Status delete_sequence([[maybe_unused]] SequenceId id,
                       [[maybe_unused]] Token token) {
    return Status::OK;
}

} // namespace shirakami
