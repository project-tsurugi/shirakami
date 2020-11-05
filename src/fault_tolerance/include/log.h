//
// Created by thawk on 2020/11/05.
//

#pragma once

#include <string>

#include "boost/filesystem.hpp"

namespace shirakami {

class Log {
public:
    [[nodiscard]] static std::string &get_kLogDirectory() {  // NOLINT
        return kLogDirectory;
    }

    static void set_kLogDirectory(std::string_view new_directory) {
        kLogDirectory.assign(new_directory);
    }

    [[maybe_unused]] static void recovery_from_log();

private:
    static inline std::string kLogDirectory{};  // NOLINT
};

} // namespace shirakami::log
