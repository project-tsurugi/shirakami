#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string_view>

#include "scheme.h"
#include "storage_options.h"

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
    UPSERT,
    ADD_STORAGE,
    REMOVE_STORAGE,
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
        case log_operation::UPSERT:
            return "UPSERT";
        case log_operation::ADD_STORAGE:
            return "ADD_STORAGE";
        case log_operation::REMOVE_STORAGE:
            return "REMOVE_STORAGE";
    }
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

    log_record(log_operation operation, std::string_view key,
               std::string_view value, std::uint64_t major_version,
               std::uint64_t minor_version, storage_id_type storage_id)
        : operation_(operation), key_(key), value_(value),
          major_version_(major_version), minor_version_(minor_version),
          storage_id_(storage_id) {}

    [[nodiscard]] log_operation get_operation() const { return operation_; }

    [[nodiscard]] std::string_view get_key() const { return key_; }

    [[nodiscard]] std::string_view get_value() const { return value_; }

    [[nodiscard]] std::uint64_t get_major_version() const {
        return major_version_;
    }

    [[nodiscard]] std::uint64_t get_minor_version() const {
        return minor_version_;
    }

    [[nodiscard]] storage_id_type get_storage_id() const { return storage_id_; }

    /**
     * @brief operation type for log record entry.
     *
     */
    log_operation operation_{}; // NOLINT

    /**
     * @brief key part of the log record
     */
    std::string_view key_{}; // NOLINT

    /**
     * @brief value part of the log record
     *
     */
    std::string_view value_{}; // NOLINT

    /**
     * @brief major version of the log record
     *
     */
    std::uint64_t major_version_{}; // NOLINT

    /**
     * @brief minor version of the log record
     *
     */
    std::uint64_t minor_version_{}; // NOLINT

    /**
     * @brief storage id where the log record is made
     */
    storage_id_type storage_id_{}; // NOLINT
};

static_assert(std::is_trivially_copyable_v<log_record>);

} // namespace shirakami
