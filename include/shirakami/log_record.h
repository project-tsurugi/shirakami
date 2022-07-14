#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string_view>

#include "scheme.h"

#include "glog/logging.h"

namespace shirakami {

/**
 * @brief operation type for log entry
 * 
 */
enum class log_operation : std::uint32_t {
    UNKNOWN = 0U,
    INSERT,
    UPDATE,
    DELETE,
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(log_operation value) {
    switch (value) {
        case log_operation::UNKNOWN:
            return "UNKNOWN";
        case log_operation::INSERT:
            return "INSERT";
        case log_operation::UPDATE:
            return "UPDATE";
        case log_operation::DELETE:
            return "DELETE";
    }
    LOG(ERROR) << "programming error";
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param[out] out the target stream
 * @param[in] value the source enum value
 * @return the target stream.
 */
inline std::ostream& operator<<(std::ostream& out, log_operation value) {
    return out << to_string_view(value);
}

struct log_record {
    /**
     * @brief type for storage id
     */
    using storage_id_type = Storage;

    /**
     * @brief operation type for log record entry.
     * 
     */
    log_operation operation_{};

    /**
     * @brief key part of the log record
     */
    std::string_view key{};

    /**
     * @brief value part of the log record
     * 
     */
    std::string_view value_{};

    /**
     * @brief major version of the log record
     * 
     */
    std::uint64_t major_version_{};

    /**
     * @brief minor version of the log record
     * 
     */
    std::uint64_t minor_version_{};

    /**
     * @brief storage id where the log record is made
     */
    storage_id_type storage_id_{};
};

static_assert(std::is_trivially_copyable_v<log_record>);

} // namespace shirakami