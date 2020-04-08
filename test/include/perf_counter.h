/*
 * Copyright 2018-2018 umikongo project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <cinttypes>

const int SIZE = 8;
  
class PerfCounter {
    struct read_format {
        uint64_t nr;           /* if PERF_FORMAT_GROUP */
        uint64_t time_enabled; /* if PERF_FORMAT_TOTAL_TIME_ENABLED */
        uint64_t time_running; /* if PERF_FORMAT_TOTAL_TIME_RUNNING */
        struct {
            uint64_t value;
            uint64_t id;         /* if PERF_FORMAT_ID */
        } values[SIZE];        /* if PERF_FORMAT_GROUP */
    };

    // (Umask Value) << 8 | Event Num
    const std::uint32_t LLC_Misses = 0x412e;
    const std::uint32_t LLC_Reference = 0x4f2e;
    const std::uint32_t Branch_Misses_Retired = 0x00c5;
    const std::uint32_t Branch_Instruction_Retired = 0x00c4;
    
    const std::uint32_t L2_RQSTS_DEMAND_DATA_RD_MISS = 0x2124;
    const std::uint32_t L2_RQSTS_RFO_MISS = 0x2224;
    const std::uint32_t L2_RQSTS_CODE_RD_MISS = 0x2424;
    const std::uint32_t L2_RQSTS_ALL_DEMAND_MISS = 0x2724;
    const std::uint32_t L2_RQSTS_PF_MISS = 0x3824;
    const std::uint32_t L2_RQSTS_MISS = 0x3f24;

    const std::uint32_t L2_RQSTS_DEMAND_DATA_RD_HIT = 0x4124;
    const std::uint32_t L2_RQSTS_RFO_HIT = 0x4224;
    const std::uint32_t L2_RQSTS_CODE_RD_HIT = 0x4424;
    const std::uint32_t L2_RQSTS_PF_HIT = 0xdb24;

    const std::uint32_t L2_RQSTS_ALL_DEMAND_DATA_RD = 0xe124;
    const std::uint32_t L2_RQSTS_ALL_RFO = 0xe224;
    const std::uint32_t L2_RQSTS_ALL_CODE_RD = 0xe424;
    const std::uint32_t L2_RQSTS_ALL_DEMAND = 0xe724;
    const std::uint32_t L2_RQSTS_ALL_PF = 0xf824;
    const std::uint32_t L2_RQSTS_REFERENCES = 0xef24;
    
    const std::uint32_t DTLB_LOAD_MISSES_MISS_CAUSES_A_WALK = 0x0108;
    const std::uint32_t DTLB_LOAD_MISSES_WALK_COMPLETED = 0x0e08;
    const std::uint32_t DTLB_STORE_MISSES_MISS_CAUSES_A_WALK = 0x0149;
    const std::uint32_t DTLB_STORE_MISSES_WALK_COMPLETED = 0x0e49;

    const std::uint32_t MEM_INST_RETIRED_ALL_LOADS = 0x81d0;
    const std::uint32_t MEM_INST_RETIRED_ALL_STORES = 0x82d0;

 public:
    PerfCounter() = default;
    explicit PerfCounter(bool exc_kernel) {
        init(exc_kernel ? 1 : 0);
    }
    ~PerfCounter() = default;

    void init(int exc_kernel = 1);
    void start() {
        ioctl(fd[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    }
    void stop() {
        ioctl(fd[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        read();
    }
    void print(char const * title = "perf result") {
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
    PerfCounter();
    ~PerfCounter() = default;

    PerfCounter& operator+=(PerfCounter &rhs) {
        time_enabled += rhs.time_enabled;
        time_running += rhs.time_running;
        for(int i = 0; i < SIZE; i++) {
            val[i] += rhs.val[i];
        }
        return *this;
    }
    PerfCounter& operator/=(int n) {
        time_enabled /= n;
        time_running /= n;
        for(int i = 0; i < SIZE; i++) {
            val[i] /= n;
        }
        return *this;
    }

  private:
    int read() {
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

    uint64_t id[SIZE];
    int fd[SIZE];

    uint64_t time_enabled {};
    uint64_t time_running {};
    uint64_t val[SIZE] {};
};
