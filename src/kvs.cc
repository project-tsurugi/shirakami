/**
 * @file kvs.cc
 * @brief about entire kvs.
 */

#include "include/kvs.hh"

#include <cstdint>

#include "boost/filesystem.hpp"
#include "include/cache_line_size.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/gcollection.hh"
#include "include/header.hh"
#include "include/port.h"
#include "include/scheme.hh"
#include "include/xact.hh"
#include "kvs/interface.h"

using std::cout;
using std::endl;

namespace kvs {

std::thread kEpochThread;
std::atomic<bool> kEpochThreadEnd;
uint64_t kGlobalEpoch(1);
uint64_t kReclamationEpoch(0);
std::string LogDirectory;

static void
invoke_epocher()
{
  kEpochThreadEnd.store(false, std::memory_order_release);
  kEpochThread = std::thread(epocher);
}

static void
invoke_core_thread()
{
  invoke_epocher();
}

static void
init_kThreadTable()
{
  uint64_t ctr(0);
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    itr->visible.store(false, std::memory_order_release);
    itr->txbegan_ = false;

    /**
     * about garbage collection.
     * note : the length of kGarbageRecords is KVS_NUMBER_OF_LOGICAL_CORES.
     * So it needs surplus operation.
     */
    std::size_t gc_index = ctr % KVS_NUMBER_OF_LOGICAL_CORES;
    itr->gc_container_ = &kGarbageRecords[gc_index];
    itr->gc_container_index_ = gc_index;

    /**
     * about logging.
     */
#ifdef WAL
    itr->log_dir_.assign(LogDirectory);
    itr->log_dir_.append("/log");
    itr->log_dir_.append(std::to_string(ctr));
    if (!itr->logfile_.open(itr->log_dir_, O_CREAT | O_TRUNC | O_WRONLY, 0644)) {
      ERR;
    }
    //itr->logfile_.ftruncate(10^9); // if it want to be high performance in experiments, this line is used.
#endif
    ++ctr;
  }
}

static void
fin_kThreadTable()
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
    /**
     * about holding operation info.
     */
    itr->clean_up_ops_set();

    /**
     * about scan operation
     */
    itr->clean_up_scan_caches();

    /**
     * about logging
     */
    itr->log_set_.clear();
#ifdef WAL
    itr->logfile_.close();
#endif
  }
}

Status
init(std::string log_directory_path)
{
  /**
   * The default value of log_directory is PROJECT_ROOT.
   */
  LogDirectory.assign(log_directory_path);
  if (log_directory_path == MAC2STR(PROJECT_ROOT)) {
    LogDirectory.append("/log");
  }

  /**
   * check whether log_directory_path is filesystem objects.
   */
  boost::filesystem::path log_dir(LogDirectory);
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
  single_recovery_from_log();

  init_kThreadTable();
  invoke_core_thread();

  return Status::OK;
}

void
fin()
{
  kEpochThreadEnd.store(true, std::memory_order_release);
  kEpochThread.join();
  fin_kThreadTable();
}

void
single_recovery_from_log()
{
  std::vector<LogRecord> log_set;
  for (auto i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
    File logfile;
    std::string filename(LogDirectory);
    filename.append("/log");
    filename.append(std::to_string(i));
    if (!logfile.try_open(filename, O_RDONLY)) {
      /**
       * the file doesn't exist.
       */
      continue;
    }

    LogRecord log;
    LogHeader logheader;

    const std::size_t fix_size = sizeof(TidWord) + sizeof(OP_TYPE) + sizeof(std::size_t) + sizeof(std::size_t);
    while (sizeof(LogHeader) == logfile.read((void*)&logheader, sizeof(LogHeader))) {
      std::vector<LogRecord> log_tmp_buf;
      for (auto i = 0; i < logheader.logRecNum_; ++i) {
        if (fix_size != logfile.read((void*)&log, fix_size)) break;
        log.tuple_.key = std::make_unique<char[]>(log.tuple_.len_key);
        log.tuple_.val = std::make_unique<char[]>(log.tuple_.len_val);
        if ((log.tuple_.len_key != logfile.read((void*)log.tuple_.key.get(), log.tuple_.len_key))
            || (log.tuple_.len_val != logfile.read((void*)log.tuple_.val.get(), log.tuple_.len_val)))
          break;
        logheader.chkSum_ += log.computeChkSum();
        log_tmp_buf.emplace_back(std::move(log));
      }
      if (logheader.chkSum_ == 0) {
        for (auto itr = log_tmp_buf.begin(); itr != log_tmp_buf.end(); ++itr) {
          log_set.emplace_back(std::move(*itr));
        }
      } else {
        break;
      }
    }

    logfile.close();
  }

  /**
   * If no log files exist, it return.
   */
  if (log_set.size() == 0) return;

  sort(log_set.begin(), log_set.end());
  const std::size_t recovery_epoch = log_set.back().tid_.epoch - 2;

  Token s{};
  Storage st{};
  enter(s);
  for (auto itr = log_set.begin(); itr != log_set.end(); ++itr) {
    tbegin(s);
    if ((*itr).op_ == OP_TYPE::UPDATE || (*itr).op_ == OP_TYPE::INSERT) {
      upsert(s, st, (*itr).tuple_.key.get(), (*itr).tuple_.len_key, (*itr).tuple_.val.get(), (*itr).tuple_.len_val);
    } else if ((*itr).op_ == OP_TYPE::DELETE) {
      delete_record(s, st, (*itr).tuple_.key.get(), (*itr).tuple_.len_key);
    }
    commit(s);
  }
  leave(s);

}

}  // namespace kvs
