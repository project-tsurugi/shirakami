
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
}

void wp_meta::init() {
    clear_wped();
}

} // namespace shirakami::wp
