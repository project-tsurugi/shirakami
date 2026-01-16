
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

void wp_meta::display() {
}

void wp_meta::init() {
}

} // namespace shirakami::wp
