#pragma once

namespace shirakami {

/**
 * @brief Storage Handle
 * @details Lower 32 bits is used for user specifying id, higher 32 bits is used
 * for shirakami specifying id.
 */
using Storage = std::uint64_t;

/**
 * @brief Special storage handle.
 * @details When user uses create_storage, user can select to use 2nd arg.
 * If user uses this for that, storage id is specified by shirakami, otherwise,
 *  storage id is specified by user.
 */
constexpr Storage storage_id_undefined{UINT64_MAX};

class storage_option {
public:
    using id_t = std::uint64_t;

    // constructor
    storage_option() = default;

    storage_option(id_t id) : id_(id) {} // NOLINT

    storage_option(id_t id, std::string_view pl)
        : id_(id), payload_(pl) {} // NOLINT

    // setter / getter
    void id(id_t id) { id_ = id; }

    [[nodiscard]] id_t id() const { return id_; }

    void payload(std::string_view sv) { payload_ = sv; }

    std::string_view payload() const { return payload_; }

private:
    id_t id_{storage_id_undefined};

    std::string payload_{};
};

} // namespace shirakami