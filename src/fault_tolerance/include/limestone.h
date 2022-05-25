#pragma once

#include <string_view>

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/scheme.h"

namespace shirakami {

class write_version_type {
public:
private:
    /**
     * @brief For PITR and major write version
     * 
     */
    epoch::epoch_t epoch_number_;

    /**
     * @brief The order in the same epoch.
     * @details bit layout:
     * 1 bits: 0 - short tx, 1 - long tx.
     * 63 bits: the order between short tx or long tx id.
     */
    std::uint64_t minor_write_version_;
};

// ==========
// about data store

// begin session
// datastore::create_channel(path location) -> log channel

// check last epoch
// epoch::epoch_t datastore::last_epoch();

// switch epoch
// void datastore::switch_epoch(epoch::epoch_t ep);

// register durable callback
// datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback)

// end session
// ???

// switch epoch
// datastore::switch_epoch(epoch_id_type epoch_id)

// shut down
// datastore::shutdown() -> std::future<void>

// ==========

// ==========
// about log channel

// add entry
// void log_channel::add_entry(Storage st, std::string_view key, std::string_view val,
//               write_version_type wv);

// ==========

} // namespace shirakami::limestone