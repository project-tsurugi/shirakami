#pragma once

namespace shirakami {

class scan_handler {
public:
    using scan_cache_type = std::map<
            ScanHandle,
            std::tuple<Storage,
                       std::vector<std::tuple<const Record*,
                                              yakushima::node_version64_body,
                                              yakushima::node_version64*>>>>;
    using scan_cache_itr_type = std::map<ScanHandle, std::size_t>;
    static constexpr std::size_t scan_cache_storage_pos = 0;
    static constexpr std::size_t scan_cache_vec_pos = 1;

    [[maybe_unused]] scan_cache_type& get_scan_cache() { // NOLINT
        return scan_cache_;
    }

    [[maybe_unused]] scan_cache_itr_type& get_scan_cache_itr() { // NOLINT
        return scan_cache_itr_;
    }

private:
    scan_cache_type scan_cache_{};
    scan_cache_itr_type scan_cache_itr_{};
};

} // namespace shirakami