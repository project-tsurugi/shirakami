#pragma once

#include <filesystem>

namespace shirakami {

class database_options final {
public:
    enum class open_mode : std::uint32_t {
        CREATE = 0x001,
        RESTORE = 0x002,
        CREATE_OR_RESTORE = 0x003,
    };

    database_options() = default;

    database_options(open_mode om, std::filesystem::path log_directory_path)
        : open_mode_(om), log_directory_path_(log_directory_path) {}

    database_options(open_mode om, std::filesystem::path log_directory_path,
                     std::size_t logger_thread_num)
        : open_mode_(om), log_directory_path_(log_directory_path),
          logger_thread_num_(logger_thread_num) {}

    open_mode get_open_mode() { return open_mode_; }

    std::filesystem::path get_log_directory_path() {
        return log_directory_path_;
    }

    std::size_t get_logger_thread_num() { return logger_thread_num_; }

    void set_open_mode(open_mode om) { open_mode_ = om; }

    void set_log_directory_path(std::filesystem::path pt) {
        log_directory_path_ = pt;
    }

    void set_logger_thread_num(std::size_t num) { logger_thread_num_ = num; }

private:
    // about open mode
    open_mode open_mode_{open_mode::CREATE};

    // about logging
    std::filesystem::path log_directory_path_{""};

    /**
     * @brief todo. now, 1 thread.
     */
    std::size_t logger_thread_num_{0};
};

} // namespace shirakami