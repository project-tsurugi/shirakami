#pragma once

#include <mutex>

#include "shirakami/scheme.h"

namespace shirakami::bg_work {

class bg_commit {
public:
private:
    std::mutex mtx_cont_wait_tx;
    std::set<std::tuple<std::size_t, Token>> cont_wait_tx;
};

} // namespace shirakami::bg_work