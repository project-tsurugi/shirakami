#pragma once

#include "cpu.h"

namespace shirakami::wp {

/**
 * @brief metadata about wp attached to each table (page sets).
 * @details
 */
class alignas(CACHE_LINE_SIZE) wp_meta {
public:

    wp_meta() { init(); }

    void display();

    void init();

    // ==========

};

} // namespace shirakami::wp
