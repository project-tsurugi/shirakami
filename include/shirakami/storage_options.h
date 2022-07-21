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
    storage_option() = default;

    storage_option(std::uint64_t id) : id_(id) {} // NOLINT

    [[nodiscard]] std::uint64_t get_id() const { return id_; }

    void set_id(std::uint64_t id) { id_ = id; }

private:
    std::uint64_t id_{storage_id_undefined};
};

} // namespace shirakami