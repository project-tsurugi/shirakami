#pragma once

#include <cstdint>
#include <filesystem>

namespace shirakami {

class database_options final {
public:
    enum class open_mode : std::uint32_t {
        CREATE = 0x001,
        RESTORE = 0x002,
        CREATE_OR_RESTORE = 0x003,
        MAINTENANCE = 0x004,
    };

    database_options() = default; // NOLINT

    database_options(open_mode om) : open_mode_(om) {} // NOLINT

    database_options(open_mode om, std::filesystem::path&& log_directory_path)
        : open_mode_(om), log_directory_path_(log_directory_path) {}

    database_options(open_mode om, std::filesystem::path&& log_directory_path,

                     bool enable_logging_detail_info)
        : open_mode_(om), log_directory_path_(log_directory_path),

          enable_logging_detail_info_(enable_logging_detail_info) {}

    database_options(open_mode om, std::filesystem::path&& log_directory_path,
                     std::size_t epoch_time, bool enable_logging_detail_info)
        : open_mode_(om), log_directory_path_(log_directory_path),
          epoch_time_(epoch_time),
          enable_logging_detail_info_(enable_logging_detail_info) {}

    database_options(open_mode om, std::filesystem::path&& log_directory_path,
                     std::size_t epoch_time, bool enable_logging_detail_info,
                     std::size_t waiting_resolver_threads)
        : open_mode_(om), log_directory_path_(log_directory_path),
          epoch_time_(epoch_time),
          enable_logging_detail_info_(enable_logging_detail_info),
          waiting_resolver_threads_(waiting_resolver_threads) {}

    open_mode get_open_mode() { return open_mode_; }

    std::filesystem::path get_log_directory_path() {
        return log_directory_path_;
    }

    [[nodiscard]] std::size_t get_epoch_time() const { return epoch_time_; }

    [[nodiscard]] int get_recover_max_parallelism() const {
        return recover_max_parallelism_;
    }

    [[nodiscard]] bool get_enable_logging_detail_info() const {
        return enable_logging_detail_info_;
    }

    [[nodiscard]] std::size_t get_waiting_resolver_threads() const {
        return waiting_resolver_threads_;
    }

    [[nodiscard]] std::size_t get_index_restore_threads() const {
        return index_restore_threads_;
    }

    void set_open_mode(open_mode om) { open_mode_ = om; }

    void set_log_directory_path(std::filesystem::path& pt) {
        log_directory_path_ = pt;
    }

    void set_epoch_time(std::size_t epoch) { epoch_time_ = epoch; }

    void set_recover_max_parallelism(int num) {
        recover_max_parallelism_ = num;
    }

    void set_enable_logging_detail_info(bool tf) {
        enable_logging_detail_info_ = tf;
    }

    void set_waiting_resolver_threads(std::size_t nm) {
        waiting_resolver_threads_ = nm;
    }

    void set_index_restore_threads(std::size_t nm) {
        index_restore_threads_ = nm;
    }

private:
    // ==========
    //  about open mode
    open_mode open_mode_{open_mode::CREATE};
    // ==========

    // ==========
    // about logging
    std::filesystem::path log_directory_path_{""};

    // ==========

    // ==========
    // tuning parameter
    /**
     * @brief Parameter of epoch [us]
     */
    std::size_t epoch_time_{40000}; // NOLINT
    // ==========

    // ==========
    // about recovery
    // for limestone
    int recover_max_parallelism_{0};
    // for shirakami
    std::size_t index_restore_threads_{0};
    // ==========

    // ==========
    // detail information
    /**
     * @brief Whether it enables logging detail information.
     *
     */
    bool enable_logging_detail_info_{false};

    /**
     * @brief The number of waiting resolver threads about ltx waiting list
    */
    std::size_t waiting_resolver_threads_{2};
    // ==========
};

inline constexpr std::string_view
to_string_view(database_options::open_mode value) {
    switch (value) {
        case database_options::open_mode::CREATE:
            return "CREATE";
        case database_options::open_mode::CREATE_OR_RESTORE:
            return "CREATE_OR_RESTORE";
        case database_options::open_mode::RESTORE:
            return "RESTORE";
        case database_options::open_mode::MAINTENANCE:
            return "MAINTENANCE";
        default:
            std::abort();
    }
}

inline std::ostream& operator<<(std::ostream& out,
                                database_options::open_mode value) {
    return out << to_string_view(value);
}

inline std::ostream& operator<<(std::ostream& out, database_options options) {
    return out << std::boolalpha << "open_mode:" << options.get_open_mode()
               << ", log_directory_path:" << options.get_log_directory_path()
               << ", enable_logging_detail_info:"
               << options.get_enable_logging_detail_info()
               << ", epoch_time:" << options.get_epoch_time()
               << ", recover_max_parallelism_:"
               << options.get_recover_max_parallelism()
               << ", waiting_resolver_threads:"
               << options.get_waiting_resolver_threads();
}

} // namespace shirakami
