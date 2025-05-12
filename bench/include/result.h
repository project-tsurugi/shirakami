/**
 * @file bench/include/result.h
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <iomanip>
#include <iostream>

// shirakami-impl interface library
#include "cpu.h"

namespace shirakami {

class Result {
public:
    void addLocalAllResult(const Result& other);

    void addLocalAbortCounts(uint64_t count);

    void addLocalCommitCounts(uint64_t count);

    void addLocalAbortByOperation(uint64_t count);

    void addLocalAbortByValidation(uint64_t count);

    void addLocalCommitLatency(uint64_t count);

    void addLocalBackoffLatency(uint64_t count);

    void addLocalEarlyAborts(uint64_t count);

    void addLocalExtraReads(uint64_t count);

    void addLocalGCCounts(uint64_t count);

    void addLocalGCLatency(uint64_t count);

    void addLocalGCVersionCounts(uint64_t count);

    void addLocalGCTMTElementsCounts(uint64_t count);

    void addLocalMakeProcedureLatency(uint64_t count);

    void addLocalMemcpys(uint64_t count);

    void addLocalPreemptiveAbortsCounts(uint64_t count);

    void addLocalReadLatency(uint64_t count);

    void addLocalRtsupd(uint64_t count);

    void addLocalRtsupdChances(uint64_t count);

    void addLocalTimestampHistorySuccessCounts(uint64_t count);

    void addLocalTimestampHistoryFailCounts(uint64_t count);

    void addLocalTemperatureResets(uint64_t count);

    void addLocalTMTElementsMalloc(uint64_t count);

    void addLocalTMTElementsReuse(uint64_t count);

    void addLocalTreeTraversal(uint64_t count);

    void addLocalWriteLatency(uint64_t count);

    void addLocalValiLatency(uint64_t count);

    void addLocalValidationFailureByTid(uint64_t count);

    void addLocalValidationFailureByWritelock(uint64_t count);

    void addLocalVersionMalloc(uint64_t count);

    void addLocalVersionReuse(uint64_t count);

    static void displayEnvironmentalParameter();

    void displayAbortCounts() const;

    void displayAbortRate() const;

    void displayCommitCounts() const;

    void displayTps(size_t extime) const;

    void displayAllResult(size_t clocks_per_us, size_t extime,
                          size_t thread_num) const;

    void displayAbortByOperationRate() const;  // abort by operation rate;
    void displayAbortByValidationRate() const; // abort by validation rate;
    void displayCommitLatencyRate(size_t clocks_per_us, size_t extime,
                                  size_t thread_num) const;

    void displayBackoffLatencyRate(size_t clocks_per_us, size_t extime,
                                   size_t thread_num) const;

    void displayEarlyAbortRate() const;

    void displayExtraReads() const;

    void displayGCCounts() const;

    void displayGCLatencyRate(size_t clocks_per_us, size_t extime,
                              size_t thread_num) const;

    void displayGCTMTElementsCounts() const;

    void displayGCVersionCounts() const;

    void displayMakeProcedureLatencyRate(size_t clocks_per_us, size_t extime,
                                         size_t thread_num) const;

    void displayMemcpys() const;

    void displayOtherWorkLatencyRate(size_t clocks_per_us, size_t extime,
                                     size_t thread_num) const;

    void displayPreemptiveAbortsCounts() const;

    void displayRatioOfPreemptiveAbortToTotalAbort() const;

    void displayReadLatencyRate(size_t clocks_per_us, size_t extime,
                                size_t thread_num) const;

    void displayRtsupdRate() const;

    void displayTemperatureResets() const;

    void displayTimestampHistorySuccessCounts() const;

    void displayTimestampHistoryFailCounts() const;

    void displayTreeTraversal() const;

    void displayTMTElementMalloc() const;

    void displayTMTElementReuse() const;

    void displayWriteLatencyRate(size_t clocks_per_us, size_t extime,
                                 size_t thread_num) const;

    void displayValiLatencyRate(size_t clocks_per_us, size_t extime,
                                size_t thread_num) const;

    void displayValidationFailureByTidRate() const;

    void displayValidationFailureByWritelockRate() const;

    void displayVersionMalloc() const;

    void displayVersionReuse() const;

    [[nodiscard]] std::uint64_t& get_local_commit_counts() { // NOLINT
        return local_commit_counts_;
    }

    [[nodiscard]] std::uint64_t& get_local_abort_counts() { // NOLINT
        return local_abort_counts_;
    }

private:
    alignas(CACHE_LINE_SIZE) std::uint64_t local_abort_counts_ = 0;
    std::uint64_t local_commit_counts_ = 0;
    std::uint64_t local_abort_by_operation_ = 0;
    std::uint64_t local_abort_by_validation_ = 0;
    std::uint64_t local_commit_latency_ = 0;
    std::uint64_t local_backoff_latency_ = 0;
    std::uint64_t local_early_aborts_ = 0;
    std::uint64_t local_extra_reads_ = 0;
    std::uint64_t local_gc_counts_ = 0;
    std::uint64_t local_gc_latency_ = 0;
    std::uint64_t local_gc_version_counts_ = 0;
    std::uint64_t local_gc_TMT_elements_counts_ = 0;
    std::uint64_t local_make_procedure_latency_ = 0;
    std::uint64_t local_memcpys = 0;
    std::uint64_t local_preemptive_aborts_counts_ = 0;
    std::uint64_t local_read_latency_ = 0;
    std::uint64_t local_rtsupd_ = 0;
    std::uint64_t local_rtsupd_chances_ = 0;
    std::uint64_t local_temperature_resets_ = 0;
    std::uint64_t local_timestamp_history_fail_counts_ = 0;
    std::uint64_t local_timestamp_history_success_counts_ = 0;
    std::uint64_t local_TMT_element_malloc_ = 0;
    std::uint64_t local_TMT_element_reuse_ = 0;
    std::uint64_t local_tree_traversal_ = 0;
    std::uint64_t local_vali_latency_ = 0;
    std::uint64_t local_validation_failure_by_tid_ = 0;
    std::uint64_t local_validation_failure_by_writelock_ = 0;
    std::uint64_t local_version_malloc_ = 0;
    std::uint64_t local_version_reuse_ = 0;
    std::uint64_t local_write_latency_ = 0;
    std::uint64_t total_abort_counts_ = 0;
    std::uint64_t total_commit_counts_ = 0;
    std::uint64_t total_abort_by_operation_ = 0;
    std::uint64_t total_abort_by_validation_ = 0;
    std::uint64_t total_commit_latency_ = 0;
    std::uint64_t total_backoff_latency_ = 0;
    std::uint64_t total_early_aborts_ = 0;
    std::uint64_t total_extra_reads_ = 0;
    std::uint64_t total_gc_counts_ = 0;
    std::uint64_t total_gc_latency_ = 0;
    std::uint64_t total_gc_version_counts_ = 0;
    std::uint64_t total_gc_TMT_elements_counts_ = 0;
    std::uint64_t total_make_procedure_latency_ = 0;
    std::uint64_t total_memcpys = 0;
    std::uint64_t total_preemptive_aborts_counts_ = 0;
    std::uint64_t total_read_latency_ = 0;
    std::uint64_t total_rtsupd_ = 0;
    std::uint64_t total_rtsupd_chances_ = 0;
    std::uint64_t total_temperature_resets_ = 0;
    std::uint64_t total_timestamp_history_fail_counts_ = 0;
    std::uint64_t total_timestamp_history_success_counts_ = 0;
    std::uint64_t total_TMT_element_malloc_ = 0;
    std::uint64_t total_TMT_element_reuse_ = 0;
    std::uint64_t total_tree_traversal_ = 0;
    std::uint64_t total_vali_latency_ = 0;
    std::uint64_t total_validation_failure_by_tid_ = 0;
    std::uint64_t total_validation_failure_by_writelock_ = 0;
    std::uint64_t total_version_malloc_ = 0;
    std::uint64_t total_version_reuse_ = 0;
    std::uint64_t total_write_latency_ = 0;
};

} // namespace shirakami
