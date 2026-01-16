
#include <algorithm>
#include <array>
#include <cstddef>
#include <emmintrin.h>
#include <map>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/wp_lock.h"
#include "concurrency_control/include/wp_meta.h"

#include "shirakami/logging.h"
#include "shirakami/scheme.h"

#include "glog/logging.h"

namespace shirakami::wp {

bool wp_meta::empty(const wp_meta::wped_type&) {
    return true;
}

void wp_meta::display() {
    for (std::size_t i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
        if (get_wped_used().test(i)) {
            LOG(INFO) << "epoch:\t" << get_wped().at(i).first << ", id:\t"
                      << get_wped().at(i).second;
        }
    }
}

void wp_meta::init() {
    clear_wped();
    wped_used_.reset();
}

wp_meta::wped_type wp_meta::get_wped() {
    wped_type r_obj{};
    return r_obj;
}

epoch::epoch_t wp_meta::find_min_ep(const wp_meta::wped_type&) {
    epoch::epoch_t min_ep{0};
    return min_ep;
}

std::pair<epoch::epoch_t, std::size_t>
wp_meta::find_min_ep_id(const wp_meta::wped_type&) {
    std::size_t min_id{0};
    std::size_t min_ep{0};
    return {min_ep, min_id};
}

std::size_t wp_meta::find_min_id(const wp_meta::wped_type&) {
    std::size_t min_id{0};
    return min_id;
}

} // namespace shirakami::wp
