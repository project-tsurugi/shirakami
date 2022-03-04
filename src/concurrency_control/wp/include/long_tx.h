#pragma once

#include <cstddef>

namespace shirakami::wp::batch {

/**
 * @brief The counter serving batch id which show priority of batchs.
 * @pre @a counter > 0
 */
inline std::size_t counter{1};

[[maybe_unused]] static std::size_t get_counter() { return counter; }

[[maybe_unused]] static void set_counter(std::size_t const num) { counter = num; };

} // namespace shirakami::wp::long