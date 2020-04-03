/**
 * @file log.cc
 * @brief implement about log
 */

#include "include/log.hh"

namespace kvs {

std::string LogDirectory;

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

