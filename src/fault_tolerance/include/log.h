//
// Created by thawk on 2020/11/05.
//

#pragma once

#include <string>

#if defined(CPR)

#include "cpr.h"

#endif

#include "boost/filesystem.hpp"

namespace shirakami {

class Log {
public:
    [[nodiscard]] static std::string get_kLogDirectory() {  // NOLINT
        return kLogDirectory;
    }

    static void set_kLogDirectory(std::string_view new_directory) {
        kLogDirectory.assign(new_directory);
#if defined(CPR)
        cpr::set_checkpoint_path(kLogDirectory + "/checkpoint");
        cpr::set_checkpointing_path(kLogDirectory + "/checkpointing");
#endif
    }

    [[maybe_unused]] static void recovery_from_log();

private:
    static inline std::string kLogDirectory{};  // NOLINT
};

} // namespace shirakami::log
