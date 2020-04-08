/**
 * @file log.cc
 * @brief implement about log
 */

#include "log.hh"
#include "kvs/scheme.h"

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
    std::vector<Tuple> tuple_buffer;

    const std::size_t fix_size = sizeof(TidWord) + sizeof(OP_TYPE);
    while (sizeof(LogHeader) == logfile.read((void*)&logheader, sizeof(LogHeader))) {
      std::vector<LogRecord> log_tmp_buf;
      for (auto i = 0; i < logheader.logRecNum_; ++i) {
        if (fix_size != logfile.read((void*)&log, fix_size)) break;
        std::unique_ptr<char[]> key_ptr, value_ptr;
        std::size_t key_length, value_length;
        // read key_length
        if (sizeof(std::size_t) != logfile.read((void*)&key_length, sizeof(std::size_t))) break;
        // read key_body
        key_ptr = std::make_unique<char[]>(key_length);
        if (key_length != logfile.read((void*)key_ptr.get(), key_length)) break;
        // read value_length
        if (sizeof(std::size_t) != logfile.read((void*)&value_length, sizeof(std::size_t))) break;
        // read value_body
        value_ptr = std::make_unique<char[]>(value_length);
        if (value_length != logfile.read((void*)value_ptr.get(), value_length)) break;

        
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
  const Epoch recovery_epoch = log_set.back().tid_.epoch - 2;

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

