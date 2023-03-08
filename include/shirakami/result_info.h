#pragma once

#include <cstdlib>
#include <string>
#include <string_view>

#include "binary_printer.h"
#include "storage_options.h"

namespace shirakami {

/**
 * @brief Reason code. This is a secondary information about failed result after
 * commit command.
 */
enum class reason_code : std::int32_t {
    /**
     * @brief undefined code.
     */
    UNKNOWN,
    /**
     * @brief You tried to delete a nonexistent record. If the record remains 
     * non-existent, delete operations on that record will continue to fail.
     */
    KVS_DELETE,
    /**
     * @brief You tried to insert on a key that already exists. If that key 
     * continues to exist, your insert operation will continue to fail.
     */
    KVS_INSERT,
    /**
     * @brief You tried to update a record with a nonexistent key. If the 
     * record remains non-existent, your update operation will continue to fail.
     */
    KVS_UPDATE,
    /**
     * @brief Abort due to phantom avoidance function in the ltx.
     */
    CC_LTX_PHANTOM_AVOIDANCE,
    /**
     * @brief violation of read area at tx_begin.
     */
    CC_LTX_READ_AREA_VIOLATION,
    /**
     * @brief The low priority ltx found high priority ltx's write preserve, 
     * and tried forwarding at commit phase but the forwarding break old own 
     * read.
     */
    CC_LTX_READ_UPPER_BOUND_VIOLATION,
    /**
     * @brief Protecting committed transactional read operation from this ltx's
     *  write operation.
     */
    CC_LTX_WRITE_COMMITTED_READ_PROTECTION,
    /**
     * @brief Occ tx detected write preserve of ltx.
     */
    CC_OCC_WP_VERIFY,
    /**
     * @brief Occ tx failed read validation due to overwrite.
     */
    CC_OCC_READ_VERIFY,
    /**
     * @brief Abort due to phantom avoidance function in the occ transaction.
     */
    CC_OCC_PHANTOM_AVOIDANCE,
    /**
     * @brief After abort command by user.
     */
    USER_ABORT,
};

inline constexpr std::string_view to_string_view(reason_code rc) noexcept {
    using namespace std::string_view_literals;
    switch (rc) {
        case reason_code::UNKNOWN:
            return "UNKNOWN"sv; // NOLINT
        case reason_code::KVS_DELETE:
            return "KVS_DELETE"sv; // NOLINT
        case reason_code::KVS_INSERT:
            return "KVS_INSERT"sv; // NOLINT
        case reason_code::KVS_UPDATE:
            return "KVS_UPDATE"sv; // NOLINT
        case reason_code::CC_LTX_PHANTOM_AVOIDANCE:
            return "CC_LTX_PHANTOM_AVOIDANCE"sv; //NOLINT
        case reason_code::CC_LTX_READ_AREA_VIOLATION:
            return "CC_LTX_READ_AREA_VIOLATION"sv; // NOLINT
        case reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION:
            return "CC_LTX_READ_UPPER_BOUND_VIOLATION"sv; // NOLINT
        case reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION:
            return "CC_LTX_WRITE_COMMITTED_READ_PROTECTION"sv; // NOLINT
        case reason_code::CC_OCC_READ_VERIFY:
            return "CC_OCC_READ_VERIFY"sv; // NOLINT
        case reason_code::CC_OCC_WP_VERIFY:
            return "CC_OCC_WP_VERIFY"sv; // NOLINT
        case reason_code::CC_OCC_PHANTOM_AVOIDANCE:
            return "CC_OCC_PHANTOM_AVOIDANCE"sv; //NOLINT
        case reason_code::USER_ABORT:
            return "USER_ABORT"sv; // NOLINT
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, reason_code rc) { // NOLINT
    return out << to_string_view(rc);
}

class result_info {
public:
    result_info() = default;

    explicit result_info(reason_code rc) : reason_code_(rc) {}

    /**
     * @brief clear info.
     */
    void clear() {
        set_reason_code(reason_code::UNKNOWN);
        set_has_key_info(false);
        set_has_storage_name_info(false);
        key_.clear();          // if it use set func, bool info is set as true;
        storage_name_.clear(); // if it use set func, bool info is set as true;
    }

    // start: getter / setter

    [[nodiscard]] reason_code get_reason_code() const { return reason_code_; }

    [[nodiscard]] bool get_has_key_info() const { return has_key_info_; }

    [[nodiscard]] std::string_view get_key() const { return key_; }

    [[nodiscard]] bool get_has_storage_name_info() const {
        return has_storage_name_info_;
    }

    [[nodiscard]] std::string_view get_storage_name() const {
        return storage_name_;
    }

    void set_reason_code(reason_code rc) {
        if (rc == reason_code::UNKNOWN) { set_key(""); }
        reason_code_ = rc;
    }

    void set_has_key_info(bool tf) { has_key_info_ = tf; }

    void set_key(std::string_view key) {
        set_has_key_info(true);
        key_ = key;
    }

    void set_has_storage_name_info(bool tf) { has_storage_name_info_ = tf; }

    void set_storage_name(std::string_view name) {
        set_has_storage_name_info(true);
        storage_name_ = name;
    }

    void set_storage_name(Storage storage);

    void set_key_storage_name(std::string_view key, Storage storage);

    // end: getter / setter
private:
    reason_code reason_code_{};

    /**
     * @brief If this is true, key_ is valid.
     */
    bool has_key_info_{false};

    /**
     * @brief If this is true, storage_name_ is valid.
     */
    bool has_storage_name_info_{false};

    /**
     * @brief The reason key. reason_code::KVS_DELETE, KVS_INSERT, KVS_UPDATE, 
     * CC_LTX_PHANTOM_AVOIDANCE, CC_LTX_WRITE_COMMITTED_READ_PROTECTION, 
     * (partial) CC_OCC_PHANTOM_AVOIDANCE, CC_OCC_READ_VERIFY log this 
     * information.
     */
    std::string key_{};

    /**
     * @brief The storage name of the key.
     */
    std::string storage_name_{};
};

inline std::ostream& operator<<(std::ostream& out, result_info const& info) {
    out << "reason_code:" << info.get_reason_code();

    // output storage name info
    if (info.get_has_storage_name_info()) {
        out << ", storage_name:" << binary_printer(info.get_storage_name());
    } else {
        out << ", storage_name is not available";
    }
    // output key info
    if (info.get_has_key_info()) {
        out << ", key(len=" << info.get_key().size()
            << "):" << binary_printer(info.get_key());
    } else {
        out << ", no key information";
    }

    return out;
}

} // namespace shirakami