
#include "gtest/gtest.h"

#include "shirakami/interface.h"

// kvs_charkey-impl interface library
#include "./perf_counter.h"
#include "compiler.h"
#include "tsc.h"

using namespace shirakami;
using std::cout;
using std::endl;

namespace shirakami::testing {

const bool exc_kernel = false;
constexpr const int MAX_TUPLES = 1000000;
constexpr const int READ_TUPLES = 100;
Storage st{};

class ScanPerfTest : public ::testing::Test {
protected:
    ScanPerfTest() : v1("a") {
        init();
        register_storage(st);
        for (int i = 0; i < MAX_TUPLES; ++i) { key[i] = i; }
    }
    ~ScanPerfTest() { fin(); }
    void DoScan();
    void DoInsert(int, int);

    uint64_t key[MAX_TUPLES];
    std::string v1;
    Token s{};
    Storage st{};

    uint64_t start, end, scan_size;
};

void ScanPerfTest::DoInsert(int bgn_idx, int end_idx) {
    for (int i = bgn_idx; i < end_idx; ++i) {
        ASSERT_EQ(Status::OK, insert(s, st, (char*) &key[i], sizeof(key[i]),
                                     v1.data(), v1.size()));
    }
    ASSERT_EQ(Status::OK, commit(s));
}

void ScanPerfTest::DoScan() {
    ScanHandle handle{};
    Tuple* tuple{};
    {
        ASSERT_EQ(Status::OK, open_scan(s, st, nullptr, 0, false, nullptr, 0,
                                        false, handle));

        PerfCounter perf_counter(exc_kernel);
        perf_counter.start();
        start = rdtscp();
        for (int i = 0; i < READ_TUPLES; ++i) {
            ASSERT_EQ(Status::OK, read_key_from_scan(s, st, handle, &tuple));
            ASSERT_EQ(Status::OK, next(s, handle));
        }
        end = rdtscp();
        perf_counter.stop();

        /**
     * Make sure the scan size.
     */
        ASSERT_EQ(Status::OK,
                  scannable_total_index_size(s, st, handle, scan_size));
        ASSERT_EQ(Status::OK, commit(s));

        cout << "scannable_total_index_size : " << scan_size << endl;
        cout << "Result : " << end - start << " [clocks]" << endl;
        perf_counter.print();
    }
}

TEST_F(ScanPerfTest, read_from_scan) {
    ASSERT_EQ(Status::OK, enter(s));

    DoInsert(0, 100);
    cout << "Perform 100 records read_from_scan on a table with 100 records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(100, 1000);
    cout << "Perform 100 records read_from_scan on a table with 1K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(1000, 10000);
    cout << "Perform 100 records read_from_scan on a table with 10K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(10000, 20000);
    cout << "Perform 100 records read_from_scan on a table with 20K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(20000, 40000);
    cout << "Perform 100 records read_from_scan on a table with 40K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(40000, 80000);
    cout << "Perform 100 records read_from_scan on a table with 80K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(80000, 160000);
    cout << "Perform 100 records read_from_scan on a table with 160K records."
         << endl;
    DoScan();

    /**
   * Prepare for next experiments.
   */
    DoInsert(160000, 320000);
    cout << "Perform 100 records read_from_scan on a table with 320K records."
         << endl;
    DoScan();

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing


// perf_counter

void PerfCounter::init(int exc_kernel) {
    struct perf_event_attr pea;

    memset(&pea, 0, sizeof(struct perf_event_attr));
    pea.type = PERF_TYPE_HARDWARE;
    pea.size = sizeof(struct perf_event_attr);
    pea.config = PERF_COUNT_HW_CPU_CYCLES;
    pea.disabled = 1;
    pea.exclude_kernel = exc_kernel;
    pea.exclude_hv = 1;
    pea.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
    pea.read_format |=
            PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
    fd[0] = ::syscall(__NR_perf_event_open, &pea, 0, -1, -1, 0);
    if (fd[0] == -1) {
        perror("perf_event_open-0");
        exit(1);
    }
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
    if (fd[1] == -1) {
        perror("perf_event_open-1");
        exit(1);
    }
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
    if (fd[2] == -1) {
        perror("perf_event_open-2");
        exit(1);
    }
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
    if (fd[3] == -1) {
        perror("perf_event_open-3");
        exit(1);
    }
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
    if (fd[4] == -1) {
        perror("perf_event_open-4");
        exit(1);
    }
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
    if (fd[5] == -1) {
        perror("perf_event_open-5");
        exit(1);
    }
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
    if (fd[6] == -1) {
        perror("perf_event_open-6");
        exit(1);
    }
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
    if (fd[7] == -1) {
        perror("perf_event_open-7");
        exit(1);
    }
    ioctl(fd[7], PERF_EVENT_IOC_ID, &id[7]);

    ioctl(fd[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
}
