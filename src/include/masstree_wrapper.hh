#pragma once

#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <typeinfo>
#include <vector>

#include <pthread.h>
#include <stdlib.h>
#include <xmmintrin.h>

#include "debug.hh"
#include "header.hh"

#include "kvs/scheme.h"

/* if you use formatter, following 2 lines may be exchange.
 * but there is a dependency relation, so teh order is restricted config -> compiler.
 * To depend exchanging 2 lines, insert empty line. */
#include "../../third_party/masstree-beta/config.h"

#include "../../third_party/masstree-beta/compiler.hh"

#include "../../third_party/masstree-beta/kvthread.hh"
#include "../../third_party/masstree-beta/masstree.hh"
#include "../../third_party/masstree-beta/masstree_insert.hh"
#include "../../third_party/masstree-beta/masstree_print.hh"
#include "../../third_party/masstree-beta/masstree_remove.hh"
#include "../../third_party/masstree-beta/masstree_scan.hh"
#include "../../third_party/masstree-beta/masstree_stats.hh"
#include "../../third_party/masstree-beta/masstree_tcursor.hh"
#include "../../third_party/masstree-beta/string.hh"

using std::cout;
using std::endl;

class key_unparse_unsigned {
 public:
  static int unparse_key(Masstree::key<uint64_t> key, char* buf, int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

template <typename T>
class SearchRangeScanner {
public:
  typedef Masstree::Str Str;
  const char * const rkey_;
  const std::size_t len_rkey_;
  const bool r_exclusive_;
  std::vector<const T*>* scan_buffer_;
  const bool limited_scan_;
  const std::size_t kLimit_ = 1000;

  SearchRangeScanner(const char * const rkey, const std::size_t len_rkey, const bool r_exclusive, std::vector<const T*>* scan_buffer, bool limited_scan = false) : rkey_(rkey), len_rkey_(len_rkey), r_exclusive_(r_exclusive), scan_buffer_(scan_buffer), limited_scan_(limited_scan) {
    if (limited_scan) {
      scan_buffer->reserve(kLimit_);
    }
  }

  template <typename SS, typename K>
  void visit_leaf(const SS&, const K&, threadinfo&) {}

  bool visit_value(const Str key, T* val, threadinfo&) {
    if (limited_scan_) {
      if (scan_buffer_->size() >= kLimit_) {
        return false;
      }
    }

    if (rkey_ == nullptr) {
      scan_buffer_->emplace_back(val);
      return true;
    }

    const int res_memcmp = memcmp(rkey_, key.s, std::min(len_rkey_, static_cast<std::size_t>(key.len)));
    if (res_memcmp > 0
        || (res_memcmp == 0 && 
        ((!r_exclusive_ && len_rkey_ == static_cast<std::size_t>(key.len)) || len_rkey_ > static_cast<std::size_t>(key.len)))) {
      scan_buffer_->emplace_back(val);
      return true;
    } else {
      return false;
    }
  }
};

/* Notice.
 * type of object is T.
 * inserting a pointer of T as value.
 */
template <typename T>
class MasstreeWrapper {
 public:
  static constexpr uint64_t insert_bound = UINT64_MAX;  // 0xffffff;
  // static constexpr uint64_t insert_bound = 0xffffff; //0xffffff;
  struct table_params : public Masstree::nodeparams<15, 15> {
    typedef T* value_type;
    typedef Masstree::value_print<value_type> value_print_type;
    typedef threadinfo threadinfo_type;
    typedef key_unparse_unsigned key_unparse_type;
    static constexpr ssize_t print_max_indent_depth = 12;
  };

  typedef Masstree::Str Str;
  typedef Masstree::basic_table<table_params> table_type;
  typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
  typedef Masstree::tcursor<table_params> cursor_type;
  typedef Masstree::leaf<table_params> leaf_type;
  typedef Masstree::internode<table_params> internode_type;

  typedef typename table_type::node_type node_type;
  typedef typename unlocked_cursor_type::nodeversion_value_type
      nodeversion_value_type;

  static __thread typename table_params::threadinfo_type* ti;

  MasstreeWrapper() { this->table_init(); }

  void table_init() {
    // printf("masstree table_init()\n");
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
    stopping = false;
    printing = 0;
  }

  static void thread_init(int thread_id) {
    if (ti == nullptr) ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  }

  void table_print() {
    table_.print(stdout);
    fprintf(stdout, "Stats: %s\n",
            Masstree::json_stats(table_, ti)
                .unparse(lcdf::Json::indent_depth(1000))
                .c_str());
  }

  /**
   * @brief insert value to masstree
   * @param key This must be a type of const char*.
   * @detail future work, we try to delete making temporary
   * object std::string buf(key). But now, if we try to do 
   * without making temporary object, it fails by masstree.
   * @return Status::WARN_ALREADY_EXISTS The records whose key is the same as @key exists in masstree, so this function returned immediately.
   * @return Status::OK success.
   */
  kvs::Status insert_value(const char* key, std::size_t len_key, T* value) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_insert(*ti);
    // always_assert(!found, "keys should all be unique");
    if (found) {
      // release lock of existing nodes meaning the first arg equals 0
      lp.finish(0, *ti);
      // return
      return kvs::Status::WARN_ALREADY_EXISTS;
    }
    lp.value() = value;
    fence();
    lp.finish(1, *ti);
    return kvs::Status::OK;
  }

  // for bench.
  kvs::Status put_value(const char* key, std::size_t len_key, T* value, T** record) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(*ti);
    if (found) {
      *record = lp.value();
      *value = **record;
      lp.value() = value;
      fence();
      lp.finish(0, *ti);
      return kvs::Status::OK;
    } else {
      fence();
      lp.finish(0, *ti);
      /**
       * Look project_root/third_party/masstree/masstree_get.hh:98 and 100.
       * If the node wasn't found, the lock was acquired and tcursor::state_ is 0.
       * So it needs to release.
       * If state_ == 0, finish function merely release locks of existing nodes.
       */
      return kvs::Status::WARN_NOT_FOUND;
    }
  }

  kvs::Status remove_value(const char* key, std::size_t len_key) {
    cursor_type lp(table_, key, len_key);
    bool found = lp.find_locked(*ti);
    if (found) {
      // try finish_remove. If it fails, following processing unlocks nodes.
      lp.finish(-1, *ti);
      return kvs::Status::OK;
    } else {
      // no nodes
      lp.finish(-1, *ti);
      return kvs::Status::WARN_NOT_FOUND;
    }
  }

  T* get_value(const char* key, std::size_t len_key) {
    unlocked_cursor_type lp(table_, key, len_key);
    bool found = lp.find_unlocked(*ti);
    if (found) return lp.value();
    else return nullptr;
  }

  void scan(const char * const lkey, const std::size_t len_lkey, const  bool l_exclusive, const char * const rkey, const std::size_t len_rkey, const bool r_exclusive, std::vector<const T*>* res, bool limited_scan = false) {
    Str mtkey;
    if (lkey == nullptr) {
      mtkey = Str();
    } else {
      mtkey = Str(lkey, len_lkey);
    }

    SearchRangeScanner<T> scanner(rkey, len_rkey, r_exclusive, res, limited_scan);
    table_.scan(mtkey, !l_exclusive, scanner, *ti);
  }

  void print_table() {
    // future work.
  }

  static bool stopping;
  static uint32_t printing;

 private:
  table_type table_;

  static inline Str make_key(const char* char_key, std::string& buf) {
    return Str(buf);
  }
};

template <typename T>
__thread typename MasstreeWrapper<T>::table_params::threadinfo_type*
    MasstreeWrapper<T>::ti = nullptr;
template <typename T>
bool MasstreeWrapper<T>::stopping = false;
template <typename T>
uint32_t MasstreeWrapper<T>::printing = 0;
extern volatile mrcu_epoch_type active_epoch;
extern volatile uint64_t globalepoch;
extern volatile bool recovering;
