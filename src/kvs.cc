/**
 * @file kvs.cc
 * @brief about entire kvs.
 */

#include "kvs.hh"

#include <cstdint>

#include "boost/filesystem.hpp"
#include "epoch.hh"
#include "gcollection.hh"
#include "log.hh"
#include "tuple.hh"
#include "xact.hh"

namespace kvs {

static void invoke_core_thread() { invoke_epocher(); }

static void init_kThreadTable() {
  uint64_t ctr(0);
  for (auto&& itr : kThreadTable) {
    itr.set_visible(false);
    itr.set_txbegan(false);

    /**
     * about garbage collection.
     * note : the length of kGarbageRecords is KVS_NUMBER_OF_LOGICAL_CORES.
     * So it needs surplus operation.
     */
    std::size_t gc_index = ctr % KVS_NUMBER_OF_LOGICAL_CORES;
    itr.set_gc_container_index(gc_index);
    itr.set_gc_record_container( &kGarbageRecords.at(gc_index));
    itr.set_gc_value_container(&kGarbageValues.at(gc_index));

    /**
     * about logging.
     */
#ifdef WAL
    itr->log_dir_.assign(kLogDirectory);
    itr->log_dir_.append("/log");
    itr->log_dir_.append(std::to_string(ctr));
    if (!itr->logfile_.open(itr->log_dir_, O_CREAT | O_TRUNC | O_WRONLY,
                            0644)) {
      ERR;
    }
    // itr->logfile_.ftruncate(10^9); // if it want to be high performance in
    // experiments, this line is used.
#endif
    ++ctr;
  }
}

static void fin_kThreadTable() {
  for (auto&& itr : kThreadTable) {
    /**
     * about holding operation info.
     */
    itr.clean_up_ops_set();

    /**
     * about scan operation
     */
    itr.clean_up_scan_caches();

    /**
     * about logging
     */
    itr.get_log_set().clear();
#ifdef WAL
    itr->logfile_.close();
#endif
  }
}

Status init(std::string_view log_directory_path) { // NOLINT
  /**
   * The default value of log_directory is PROJECT_ROOT.
   */
  Log::set_kLogDirectory(log_directory_path);
  if (log_directory_path == MAC2STR(PROJECT_ROOT)) {
    Log::get_kLogDirectory().append("/log");
  }

  /**
   * check whether log_directory_path is filesystem objects.
   */
  boost::filesystem::path log_dir(Log::get_kLogDirectory());
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

  init_kThreadTable();
  invoke_core_thread();

  return Status::OK;
}

void fin() {
  release_all_heap_objects();

  // Stop DB operation.
  kEpochThreadEnd.store(true, std::memory_order_release);
  kEpochThread.join();
  fin_kThreadTable();
}

}  // namespace kvs
