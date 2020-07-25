/**
 * @file interface.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "boost/filesystem.hpp"
#ifdef CC_SILO_VARIANT
#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/helper.h"
#include "cc/silo_variant/include/thread_info_table.h"
#endif  // CC_SILO_VARIANT
#ifdef INDEX_KOHLER_MASSTREE
#include "index/masstree_beta/include/masstree_beta_wrapper.h"
#endif                            // INDEX_KOHLER_MASSTREE
#include "include/tuple_local.h"  // sizeof(Tuple)

namespace shirakami {

Status enter(Token& token) {  // NOLINT
  MasstreeWrapper<silo_variant::Record>::thread_init(sched_getcpu());
  return silo_variant::thread_info_table::decide_token(token);
}

void fin() {
  silo_variant::garbage_collection::release_all_heap_objects();

  // Stop DB operation.
  silo_variant::epoch::set_epoch_thread_end(true);
  silo_variant::epoch::join_epoch_thread();
  silo_variant::thread_info_table::fin_kThreadTable();
}

Status init(std::string_view log_directory_path) {  // NOLINT
  /**
   * The default value of log_directory is PROJECT_ROOT.
   */
  silo_variant::Log::set_kLogDirectory(log_directory_path);
  if (log_directory_path == MAC2STR(PROJECT_ROOT)) {
    silo_variant::Log::get_kLogDirectory().append("/log");
  }

  /**
   * check whether log_directory_path is filesystem objects.
   */
  boost::filesystem::path log_dir{silo_variant::Log::get_kLogDirectory()};
  if (boost::filesystem::exists(log_dir)) {
    /**
     * some file exists.
     * check whether it is directory.
     */
    if (!boost::filesystem::is_directory(log_dir)) {
      return Status::ERR_INVALID_ARGS;
    }
  } else {
    /**
     * directory which has log_directory_path as a file path doesn't exist.
     * it can create.
     */
    boost::filesystem::create_directories(log_dir);
  }

  /**
   * If it already exists log files, it recoveries from those.
   */
  // single_recovery_from_log();

  silo_variant::thread_info_table::init_kThreadTable();
  silo_variant::epoch::invoke_epocher();

  return Status::OK;
}

Status leave(Token token) {  // NOLINT
  for (auto&& itr : silo_variant::thread_info_table::get_thread_info_table()) {
    if (&itr == static_cast<silo_variant::ThreadInfo*>(token)) {
      if (itr.get_visible()) {
        itr.set_visible(false);
        return Status::OK;
      }
      return Status::WARN_NOT_IN_A_SESSION;
    }
  }
  return Status::ERR_INVALID_ARGS;
}

}  //  namespace shirakami
