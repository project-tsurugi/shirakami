/*
 * Copyright 2019-2019 tsurugi project.
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

#include <string_view>
#include <xmmintrin.h>

#include "atomic_wrapper.h"
#include "clock.h"
#include "compiler.h"
#include "cpu.h"
#include "random.h"
#include "tsc.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

// rocksdb
#include <rocksdb/db.h>

using namespace shirakami;
using namespace rocksdb;

/**
 * general option.
 */
DEFINE_uint32(bench_type, 0, "Do benchmarking for specific type. " // NOLINT
                             "0 is insert benchmarking."
                             "1 is batch insert benchmarking."
                             "2 is update benchmarking."
                             "3 is batch update benchmarking."
                             "4 is creating and ingesting sst files. note that this bench is done by single thread.");
constexpr std::size_t bench_type_i = 0;
constexpr std::size_t bench_type_bi = 1;
constexpr std::size_t bench_type_u = 2;
constexpr std::size_t bench_type_bu = 3;
constexpr std::size_t bench_type_ci_sst = 4;
DEFINE_uint32(batch_write_num, 1, "The # of insertion as a batch.");                // NOLINT
DEFINE_uint64(cpumhz, 2000, "Cpu MHz used by measuring time.");                     // NOLINT
DEFINE_uint64(duration, 1, "Duration of benchmark in seconds.");                    // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                                      // NOLINT
DEFINE_string(bt4_sst_path, "/tmp/rocksdb_test_sst", "The option for batch_type 4." // NOLINT
                                                     "Where to make the SST file T.");
DEFINE_uint64(bt4_cont_res, 100 * 1000 * 1000, "The # of elements which is reserved for batch type 4."); // NOLINT

/**
 * rocks db options
 */
DEFINE_uint64(rocksdb_memtable_memory_budget, 512 * 1024 * 1024, // NOLINT
              "Use for Options::optimizeLevelStyleCompaction.");
DEFINE_uint64(rocksdb_options_increase_parallelism, 16, // NOLINT
              "The argument of ROCKSDB_NAMESPACE::Options.IncreaseParallelism.");
DEFINE_bool(rocksdb_options_create_if_missing, true, // NOLINT
            "The value of ROCKSDB_NAMESPACE::Options.create_if_missing.");
DEFINE_bool(rocksdb_options_direct_io_for_flush_and_compaction, false, // NOLINT
            "The value of ROCKSDB_NAMESPACE::Options.direct_io_for_flush_and_compaction.");
DEFINE_string(rocksdb_path, "/tmp/rocksdbtest", "Path to db for benchmarking."); // NOLINT

DB* db;                             // NOLINT
std::vector<std::string> bt_4_cont; // NOLINT
// resource for batch type 4.

static bool isReady(const std::vector<char>& readys); // NOLINT
static void invoke_leader();

static void waitForReady(const std::vector<char>& readys);

static void worker(size_t thid, char& ready, const bool& start, const bool& quit, std::uint64_t& res);

static void prepare_bench() {
    std::cout << "Start prepare bench" << std::endl;
    bt_4_cont.reserve(FLAGS_bt4_cont_res);
    for (std::size_t i = 0; i < FLAGS_bt4_cont_res; ++i) {
        std::string k{reinterpret_cast<char*>(&i), sizeof(i)}; // NOLINT
        bt_4_cont.emplace_back(std::move(k));
    }
    std::sort(bt_4_cont.begin(), bt_4_cont.end());
    std::cout << "End prepare bench" << std::endl;
}

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<std::uint64_t> res(FLAGS_thread); // NOLINT

    if (FLAGS_bench_type == bench_type_ci_sst) prepare_bench();

    std::vector<char> readys(FLAGS_thread); // NOLINT
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < FLAGS_thread; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res.at(i)));
    }
    waitForReady(readys);
    std::cout << "start rocksdb exp." << std::endl;
    storeRelease(start, true);
    for (size_t i = 0; i < FLAGS_duration; ++i) {
        sleepMs(1000); // NOLINT
    }
    std::cout << "stop rocksdb exp." << std::endl;
    storeRelease(quit, true);
    for (auto& th : thv) th.join();

    std::uint64_t sum{0};
    for (auto&& elem : res) {
        sum += elem;
    }
    if (FLAGS_bench_type == bench_type_ci_sst) {
        std::cout << "Throughput[ops/s]: " << sum << std::endl;
    } else {
        std::cout << "Throughput[ops/s]: " << sum / FLAGS_duration << std::endl;
    }
}

static void load_flags() {
    if (FLAGS_thread >= 1) {
        std::cout << "FLAGS_thread : " << FLAGS_thread << std::endl;
    } else {
        LOG(FATAL) << "Number of threads must be larger than 0.";
    }
    if (FLAGS_duration >= 1) {
        std::cout << "FLAGS_duration : " << FLAGS_duration << std::endl;
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }
    std::cout << "Fin load_flags()" << std::endl;
}

void set_rocksdb_options(Options& options) {
    options.IncreaseParallelism(FLAGS_rocksdb_options_increase_parallelism);
    options.OptimizeLevelStyleCompaction(FLAGS_rocksdb_memtable_memory_budget);
    options.create_if_missing = FLAGS_rocksdb_options_create_if_missing;
    options.use_direct_io_for_flush_and_compaction = FLAGS_rocksdb_options_direct_io_for_flush_and_compaction;
}

int main(int argc, char* argv[]) { // NOLINT
    google::InitGoogleLogging("shirakami-bench-rocksdb_bench");
    gflags::SetUsageMessage(static_cast<const std::string&>("RocksDB benchmark")); // NOLINT
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    load_flags();

    Options options;
    set_rocksdb_options(options);
    auto s = DB::Open(options, FLAGS_rocksdb_path, &db);
    if (!s.ok()) {
        LOG(FATAL) << "rocksdb's error code " << s.code();
    }

    std::cout << "Create db." << std::endl;
    invoke_leader();
    std::cout << "Fin measurement." << std::endl;

    delete db; // NOLINT
    std::cout << "Fin deleting db." << std::endl;
    return 0;
}

bool isReady(const std::vector<char>& readys) { // NOLINT
    for (const char& b : readys) {              // NOLINT
        if (loadAcquire(b) == 0) return false;
    }
    return true;
}

void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) {
        _mm_pause();
    }
}

void bench_insert_process(std::uint64_t insert_end, std::uint64_t& insert_cursor) {
    std::string_view key{reinterpret_cast<const char*>(&insert_cursor), sizeof(insert_cursor)}; // NOLINT
    std::string_view val{key};
    rocksdb::Slice rkey{key.data(), key.size()};
    rocksdb::Slice rval{val.data(), val.size()};
    auto s = db->Put(WriteOptions(), rkey, rval);
    if (!s.ok()) {
        LOG(FATAL) << "rocksdb's error code " << s.code();
    }
    ++insert_cursor;
    if (insert_cursor == insert_end) {
        LOG(FATAL) << "Happen round-trip problem by too long experiment time.";
    }
}

void bench_batch_insert_process(std::uint64_t insert_end, std::uint64_t& insert_cursor) {
    std::vector<std::uint64_t> vec;
    vec.reserve(FLAGS_batch_write_num);
    WriteBatch batch; // NOLINT
    for (std::size_t i = 0; i < FLAGS_batch_write_num; ++i) {
        vec.emplace_back(insert_cursor);
        std::string_view key{reinterpret_cast<const char*>(&vec.at(i)), sizeof(vec.at(i))}; // NOLINT
        std::string_view val{key};
        rocksdb::Slice rkey{key.data(), key.size()};
        rocksdb::Slice rval{val.data(), val.size()};
        batch.Put(rkey, rval);
        ++insert_cursor;
    }
    Status s = db->Write(WriteOptions(), &batch);
    if (!s.ok()) {
        LOG(FATAL) << "rocksdb's error code " << s.code();
    }
    if (insert_cursor == insert_end) {
        LOG(FATAL) << "Happen round-trip problem by too long experiment time.";
    }
}

void bench_update_process(std::uint64_t write_start, Xoroshiro128Plus& rnd) {
    std::uint64_t kv{write_start + (rnd.next() % (UINT64_MAX / FLAGS_thread))};
    std::string_view key{reinterpret_cast<const char*>(&kv), sizeof(kv)}; // NOLINT
    std::string_view val{key};
    rocksdb::Slice rkey{key.data(), key.size()};
    rocksdb::Slice rval{val.data(), val.size()};
    auto s = db->Put(WriteOptions(), rkey, rval);
    if (!s.ok()) {
        LOG(FATAL) << "rocksdb's error code " << s.code();
    }
}

void bench_batch_update_process(std::uint64_t write_start, Xoroshiro128Plus& rnd) {
    std::vector<std::uint64_t> vec;
    vec.reserve(FLAGS_batch_write_num);
    WriteBatch batch; // NOLINT
    for (std::size_t i = 0; i < FLAGS_batch_write_num; ++i) {
        vec.emplace_back(write_start + (rnd.next() % (UINT64_MAX / FLAGS_thread)));
        std::string_view key{reinterpret_cast<const char*>(&vec.at(i)), sizeof(vec.at(i))}; // NOLINT
        std::string_view val{key};
        rocksdb::Slice rkey{key.data(), key.size()};
        rocksdb::Slice rval{val.data(), val.size()};
        batch.Put(rkey, rval);
    }
    Status s = db->Write(WriteOptions(), &batch);
    if (!s.ok()) {
        LOG(FATAL) << "rocksdb's error code " << s.code();
    }
}

void bench_create_ingest_sst(std::size_t const thid, std::uint64_t& sum, const bool& quit) {
    Options options;
    SstFileWriter sst_file_writer(EnvOptions(), options);
    // Path to where we will write the SST file
    Status s = sst_file_writer.Open(FLAGS_bt4_sst_path);
    if (!s.ok()) {
        LOG(FATAL) << "can't open sst file.";
    } else if (sst_file_writer.FileSize() != 0) {
        LOG(FATAL) << "error : file size is " << sst_file_writer.FileSize();
    }

    // insert rows into the sst file, note that inserted keys must be
    // strictly increasing (based on options.comparator)
    std::cout << "Start put to sst file." << std::endl;
    for (auto&& elem : bt_4_cont) {
        s = sst_file_writer.Put(elem, elem);
        if (!s.ok()) {
            LOG(FATAL) << "Error while adding key: " << elem;
        }
        ++sum;
        if (sum == bt_4_cont.size()) {
            LOG(FATAL) << "lack of preserve. " << bt_4_cont.size();
        }
        if (loadAcquire(quit)) break;
    }
    std::cout << "Fin put to sst file." << std::endl;

    // begin finish
    std::uint64_t begin_ts{rdtscp()};
    s = sst_file_writer.Finish();
    if (!s.ok()) {
        LOG(FATAL) << "Error while finishing file. " << s.getState();
    }
    std::uint64_t end_ts{rdtscp()};
    std::cout << "Finish consume time[us]: " << (end_ts - begin_ts) / FLAGS_cpumhz / 1000;

    // begin ingestion
    IngestExternalFileOptions ifo;
    begin_ts = rdtscp();
    s = db->IngestExternalFile({FLAGS_bt4_sst_path}, ifo);
    if (!s.ok()) {
        LOG(FATAL) << "Error while ingesting file " << s.getState();
    }
    end_ts = rdtscp();
    std::cout << "Ingesting consume time[us]: " << (end_ts - begin_ts) / FLAGS_cpumhz / 1000;
}

void worker(const std::size_t thid, char& ready, const bool& start,
            const bool& quit, std::uint64_t& res) {
    // init work
    Xoroshiro128Plus rnd;
    std::uint64_t sum{0};

    // this function can be used in Linux environment only.
    setThreadAffinity(static_cast<const int>(thid));

    // some prepare
    std::uint64_t write_start{(UINT64_MAX / FLAGS_thread) * (thid)};
    std::uint64_t write_end{(UINT64_MAX / FLAGS_thread) * (thid + 1) - 1};
    std::uint64_t insert_cursor{write_start};

    // ready
    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();

    while (likely(!loadAcquire(quit))) {
        switch (FLAGS_bench_type) { // NOLINT
            case 0:
                bench_insert_process(write_end, insert_cursor);
                break;
            case 1:
                bench_batch_insert_process(write_end, insert_cursor);
                break;
            case 2:
                bench_update_process(write_start, rnd);
                break;
            case 3:
                bench_batch_update_process(write_start, rnd);
                break;
            case 4:
                if (thid != 0) return; // this type is done by single thread.
                bench_create_ingest_sst(thid, sum, quit);
                break;
            default:
                break;
        }
        ++sum;
    }
    res = sum;
}
