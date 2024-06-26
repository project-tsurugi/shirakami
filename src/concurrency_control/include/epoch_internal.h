
#pragma once

namespace shirakami::epoch {

[[maybe_unused]] extern void epoch_thread_work();

[[maybe_unused]] extern void fin();

[[maybe_unused]] extern void init(std::size_t epoch_time);

[[maybe_unused]] extern void invoke_epoch_thread();

} // namespace shirakami::epoch
