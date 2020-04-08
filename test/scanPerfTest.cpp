
#include "gtest/gtest.h"

#include "kvs/interface.h"

#include "include/compiler.hh"
#include "include/header.hh"
#include "include/tsc.hh"
#include "include/perf_counter.h"

using namespace kvs;
using std::cout;
using std::endl;

namespace kvs_charkey::testing {

class ScanPerfTest : public ::testing::Test {
 protected:
  ScanPerfTest() { kvs::init(); }
  ~ScanPerfTest() { 
    kvs::fin(); 
    kvs::delete_all_records();
    kvs::delete_all_garbage_records();
  }
};

TEST_F(ScanPerfTest, read_from_scan) {
  uint64_t key[1000000];
  for (int i = 0; i < 1000000; ++i) {
    key[i] = i;
  }
  std::string v1("a");
  Token s{};
  ASSERT_EQ(Status::OK, enter(s));
  Storage st{};
  uint64_t start, end, scan_size;

  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 100 records." << endl;
  ScanHandle handle{};
  Tuple* tuple{};
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 100; i < 1000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 1K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 1000; i < 10000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 10K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 10000; i < 20000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 20K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 20000; i < 40000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 40K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 40000; i < 80000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 80K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 80000; i < 160000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 160K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  /**
   * Prepare for next experiments.
   */
  for (int i = 160000; i < 320000; ++i) {
    ASSERT_EQ(Status::OK, insert(s, st, (char *)&key[i], sizeof(key[i]), v1.data(), v1.size()));
  }
  ASSERT_EQ(Status::OK, commit(s));

  cout << "Perform 100 records read_from_scan on a table with 320K records." << endl;
  {
  PerfCounter perf_counter;
  perf_counter.start();
  start = rdtscp();
  ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0, false, handle));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Status::OK, read_from_scan(s, st, handle, &tuple));
  }
  /**
   * Make sure  the scan size.
   */
  ASSERT_EQ(Status::OK, scannable_total_index_size(s, st, handle, scan_size));
  cout << "scannable_total_index_size : " << scan_size << endl;
  ASSERT_EQ(Status::OK, commit(s));
  end = rdtscp();
  perf_counter.stop();
  cout << "Result : " << end - start << " [clocks]" << endl;
  perf_counter.print();
  }

  ASSERT_EQ(Status::OK, leave(s));
}

} // namespace kvs_charkey::testing

// perf_counter
#include <cstdio>
#include <iostream>

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <string.h>
#include <sys/ioctl.h>
#include <asm/unistd.h>
#include <errno.h>
#include <stdint.h>

PerfCounter::PerfCounter() {
  init();
}

void
PerfCounter::init(int exc_kernel) {
  struct perf_event_attr pea;

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_HARDWARE;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = PERF_COUNT_HW_CPU_CYCLES;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  pea.read_format |= PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
  fd[0] = ::syscall(__NR_perf_event_open, &pea, 0, -1, -1, 0);
  if (fd[0] == -1) { perror("perf_event_open-0"); exit(1); }
  ioctl(fd[0], PERF_EVENT_IOC_ID, &id[0]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_HARDWARE;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = PERF_COUNT_HW_INSTRUCTIONS;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[1] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[1] == -1) { perror("perf_event_open-1"); exit(1); }
  ioctl(fd[1], PERF_EVENT_IOC_ID, &id[1]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = LLC_Misses;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[2] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[2] == -1) { perror("perf_event_open-2"); exit(1); }
  ioctl(fd[2], PERF_EVENT_IOC_ID, &id[2]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  //  pea.config = LLC_Reference;
  pea.config = L2_RQSTS_MISS;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[3] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[3] == -1) { perror("perf_event_open-3"); exit(1); }
  ioctl(fd[3], PERF_EVENT_IOC_ID, &id[3]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = L2_RQSTS_DEMAND_DATA_RD_MISS;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[4] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[4] == -1) { perror("perf_event_open-4"); exit(1); }
  ioctl(fd[4], PERF_EVENT_IOC_ID, &id[4]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = L2_RQSTS_RFO_MISS;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[5] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[5] == -1) { perror("perf_event_open-5"); exit(1); }
  ioctl(fd[5], PERF_EVENT_IOC_ID, &id[5]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = L2_RQSTS_PF_MISS;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[6] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[6] == -1) { perror("perf_event_open-6"); exit(1); }
  ioctl(fd[6], PERF_EVENT_IOC_ID, &id[6]);

  memset(&pea, 0, sizeof(struct perf_event_attr));
  pea.type = PERF_TYPE_RAW;
  pea.size = sizeof(struct perf_event_attr);
  pea.config = MEM_INST_RETIRED_ALL_STORES;
  pea.disabled = 1;
  pea.exclude_kernel = exc_kernel;
  pea.exclude_hv = 1;
  pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
  fd[7] = ::syscall(__NR_perf_event_open, &pea, 0, -1, fd[0] /*!!!*/, 0);
  if (fd[7] == -1) { perror("perf_event_open-7"); exit(1); }
  ioctl(fd[7], PERF_EVENT_IOC_ID, &id[7]);

  ioctl(fd[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
}

int
PerfCounter::read()
{
  unsigned int i, j;
  int sz;
  char buf[SIZE*32];
  struct read_format* rf = (struct read_format*) buf;

  sz = ::read(fd[0], buf, sizeof(buf));

  time_enabled = rf->time_enabled;
  time_running = rf->time_running;
  for (i = 0; i < rf->nr; i++) {
    for (j = 0; j < SIZE; j++) {
      if (rf->values[i].id == id[j]) {
	val[j] = rf->values[i].value;
	continue;
      }
    }
  }
  return sz;
}

void
PerfCounter::start()
{
  ioctl(fd[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
}

void
PerfCounter::stop() {
  ioctl(fd[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
  read();
}

void
PerfCounter::print(char const* title) {
    std::cout << "==== " << title << " ====" << std::endl;
    if (time_enabled == time_running) {
        std::cout << "time:         " << time_enabled << std::endl;
    } else {
        std::cout << "time_enabled: " << time_enabled << std::endl;
        std::cout << "time_running: " << time_running << std::endl;
    }
    std::cout << "cpu_cycles:   " << val[0] << std::endl;
    std::cout << "instructions: " << val[1] << std::endl;
    std::cout << "counter_2:    " << val[2] << std::endl;
    std::cout << "counter_3:    " << val[3] << std::endl;
    std::cout << "counter_4:    " << val[4] << std::endl;
    std::cout << "counter_5:    " << val[5] << std::endl;
    std::cout << "counter_6:    " << val[6] << std::endl;
    std::cout << "counter_7:    " << val[7] << std::endl;
    std::cout << "=====================" << std::endl;
}
