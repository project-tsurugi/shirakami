
#include "logger.h"

#include "gtest/gtest.h"

using namespace shirakami::logger;

namespace shirakami::testing {

class logger_test : public ::testing::Test {  // NOLINT
};

void spdlog_debug_output() {
    SPDLOG_DEBUG("spdlog_debug_output_func.");
}

TEST_F(logger_test, double_initialization) {  // NOLINT
    setup_spdlog();
    setup_spdlog();
    SPDLOG_DEBUG("after double initialization.");
}

TEST_F(logger_test, no_init_output_and_output_after_init) { // NOLINT
    spdlog_debug_output();
    SPDLOG_DEBUG("after first spdlog output."); // It must not output.
    setup_spdlog();
    spdlog_debug_output();
    SPDLOG_DEBUG("after second spdlog output.");
}

}  // namespace shirakami::testing
