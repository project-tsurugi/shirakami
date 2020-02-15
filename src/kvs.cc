
#include "include/cache_line_size.hh"
#include "include/cpu.hh"
#include "include/debug.hh"
#include "include/header.hh"
#include "include/kvs.hh"
#include "include/port.h"
#include "include/scheme.hh"
#include "include/xact.hh"

#include <cstdint>
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
#ifdef WAL
    itr->log_dir_.assign(LogDirectory);
    itr->log_dir_.append("/log");
    itr->log_dir_.append(std::to_string(ctr));
    ++ctr;
    if (!itr->logfile_.open(itr->log_dir_, O_CREAT | O_TRUNC | O_WRONLY, 0644)) {
      ERR;
    }
    //itr->logfile_.ftruncate(10^9); // if it want to be high performance in experiments, this line is used.
#endif
  }
}

static void
fin_kThreadTable()
{
  for (auto itr = kThreadTable.begin(); itr != kThreadTable.end(); ++itr) {
#ifdef WAL
    itr->logfile_.close();
#endif
  }
}

void
init(std::string log_directory_path)
{
  /**
   * The default value of log_directory is PROJECT_ROOT.
   */
  LogDirectory.assign(log_directory_path);
  LogDirectory.append("/log");

  init_kThreadTable();
  invoke_core_thread();
}

void
fin()
{
  kEpochThreadEnd.store(true, std::memory_order_release);
  kEpochThread.join();
  fin_kThreadTable();
}

#if 0
/**
 * Should this function be deleted ?
 */
void 
change_wal_directory(std::string new_path)
{
  LogDirectory.assign(new_path);
}
#endif

void
single_recovery_from_log()
{
  std::vector<LogRecord> log_set;
  for (auto i = 0; i < KVS_MAX_PARALLEL_THREADS; ++i) {
    File logfile;
    std::string filename(LogDirectory);
    filename.append("/log");
    filename.append(std::to_string(i));
    if (!logfile.try_open(filename, O_RDONLY)) continue;

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
