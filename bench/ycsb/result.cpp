#include <cstdint>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <cstddef>

// shirakami-impl interface library
#include "memory.h"
#include "result.h"
#include "concurrency_control/include/epoch.h"

namespace shirakami {

/**
 * @brief RAII holder to save and restore the format flags of an ostream.
 */
class FmtHolder {
public:
    FmtHolder() = delete;
    FmtHolder(const FmtHolder&) = delete;
    FmtHolder& operator=(const FmtHolder&) = delete;
    FmtHolder(FmtHolder&&) = delete;
    FmtHolder& operator=(FmtHolder&&) = delete;

    explicit FmtHolder(std::ostream& ref) :
        ref_(ref)
    {
        init_.copyfmt(ref);
    }

    ~FmtHolder() {
        ref_.copyfmt(init_);
    }

private:
    std::ostream& ref_;
    std::ios init_{nullptr};
};

void Result::displayAbortCounts() const {
    std::cout << "abort_counts_:\t" << total_abort_counts_ << std::endl;
}

void Result::displayAbortRate() const {
    if (total_abort_counts_ == 0) {
        std::cout << "abort_rate:\t0" << std::endl;
    } else {
        constexpr int prec = 8;
        long double ave_rate =
                static_cast<double>(total_abort_counts_) /
                static_cast<double>(total_commit_counts_ + total_abort_counts_);
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(prec) << "abort_rate:\t"
                  << ave_rate << std::endl;
    }
}

void Result::displayCommitCounts() const {
    std::cout << "commit_counts_:\t" << total_commit_counts_ << std::endl;
}

void Result::displayEnvironmentalParameter() {
    std::cout << "epoch_duration[us]:\t" << epoch::get_global_epoch_time_us()
              << std::endl;
}

void Result::displayTps(size_t extime) const {
    if (total_commit_counts_ == 0) {
        std::cout << "throughput[tps]:\t0" << std::endl;
    } else {
        long double result{static_cast<long double>(total_commit_counts_) /
                           extime};
        constexpr std::uint64_t ns_sec = 1000000000;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4);
        std::cout << "latency[ns]:\t" << ns_sec / result << std::endl;
        std::cout << "throughput[tps]:\t" << result << std::endl;
    }
}

void Result::displayAbortByOperationRate() const {
    if (total_abort_by_operation_ != 0U) {
        long double rate = static_cast<long double>(total_abort_by_operation_) /
                           static_cast<long double>(total_abort_counts_);
        std::cout << "abort_by_operation:\t" << total_abort_by_operation_
                  << std::endl;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "abort_by_operation_rate:\t" << rate << std::endl;
    }
}

void Result::displayAbortByValidationRate() const {
    if (total_abort_by_validation_ != 0U) {
        long double rate = static_cast<double>(total_abort_by_validation_) /
                           static_cast<double>(total_abort_counts_);
        std::cout << "abort_by_validation:\t" << total_abort_by_validation_
                  << std::endl;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "abort_by_validation_rate:\t" << rate << std::endl;
    }
}

void Result::displayCommitLatencyRate(
        [[maybe_unused]] size_t clocks_per_us, size_t extime,
        [[maybe_unused]] size_t thread_num) const {
    if (total_commit_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_commit_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "commit_latency_rate:\t" << rate << std::endl;
    }
}

void Result::displayBackoffLatencyRate(size_t clocks_per_us, size_t extime,
                                       size_t thread_num) const {
    if (total_backoff_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_backoff_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "backoff_latency_rate:\t" << rate << std::endl;
    }
}

void Result::displayEarlyAbortRate() const {
    if (total_early_aborts_ != 0U) {
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4) << "early_abort_rate:\t"
                  << static_cast<long double>(total_early_aborts_) /
                             static_cast<long double>(total_abort_counts_)
                  << std::endl;
    }
}

void Result::displayExtraReads() const {
    if (total_extra_reads_ != 0U) {
        std::cout << "extra_reads:\t" << total_extra_reads_ << std::endl;
    }
}

void Result::displayGCCounts() const {
    if (total_gc_counts_ != 0U) {
        std::cout << "gc_counts:\t" << total_gc_counts_ << std::endl;
    }
}

void Result::displayGCLatencyRate(size_t clocks_per_us, size_t extime,
                                  size_t thread_num) const {
    if (total_gc_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_gc_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4) << "gc_latency_rate:\t"
                  << rate << std::endl;
    }
}

void Result::displayGCTMTElementsCounts() const {
    if (total_gc_TMT_elements_counts_ != 0U) {
        std::cout << "gc_TMT_elements_counts:\t"
                  << total_gc_TMT_elements_counts_ << std::endl;
    }
}

void Result::displayGCVersionCounts() const {
    if (total_gc_version_counts_ != 0U) {
        std::cout << "gc_version_counts:\t" << total_gc_version_counts_
                  << std::endl;
    }
}

void Result::displayMakeProcedureLatencyRate(size_t clocks_per_us,
                                             size_t extime,
                                             size_t thread_num) const {
    if (total_make_procedure_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate =
                static_cast<long double>(total_make_procedure_latency_) /
                (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                 static_cast<long double>(extime)) /
                thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "make_procedure_latency_rate:\t" << rate << std::endl;
    }
}

void Result::displayMemcpys() const {
    if (total_memcpys != 0U) {
        std::cout << "memcpys:\t" << total_memcpys << std::endl;
    }
}

void Result::displayOtherWorkLatencyRate(size_t clocks_per_us, size_t extime,
                                         size_t thread_num) const {
    long double sum_rate = 0;

    if (total_make_procedure_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        sum_rate += static_cast<long double>(total_make_procedure_latency_) /
                    (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                     static_cast<long double>(extime)) /
                    thread_num;
    }
    constexpr std::size_t ten = 10;
    constexpr std::size_t six = 6;
    if (total_read_latency_ != 0U) {
        sum_rate += static_cast<long double>(total_read_latency_) /
                    (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                     static_cast<long double>(extime)) /
                    thread_num;
    }
    if (total_write_latency_ != 0U) {
        sum_rate += static_cast<long double>(total_write_latency_) /
                    (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                     static_cast<long double>(extime)) /
                    thread_num;
    }
    if (total_vali_latency_ != 0U) {
        sum_rate += static_cast<long double>(total_vali_latency_) /
                    (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                     static_cast<long double>(extime)) /
                    thread_num;
    }
    if (total_gc_latency_ != 0U) {
        sum_rate += static_cast<long double>(total_gc_latency_) /
                    (static_cast<long double>(clocks_per_us) * powl(ten, six) *
                     static_cast<long double>(extime)) /
                    thread_num;
    }

    FmtHolder fmt{std::cout};
    std::cout << std::fixed << std::setprecision(4)
              << "other_work_latency_rate:\t" << (1.0 - sum_rate) << std::endl;
}

void Result::displayPreemptiveAbortsCounts() const {
    if (total_preemptive_aborts_counts_ != 0U) {
        std::cout << "preemptive_aborts_counts:\t"
                  << total_preemptive_aborts_counts_ << std::endl;
    }
}

void Result::displayRatioOfPreemptiveAbortToTotalAbort() const {
    if (total_preemptive_aborts_counts_ != 0U) {
        long double rate =
                static_cast<double>(total_preemptive_aborts_counts_) /
                static_cast<double>(total_abort_counts_);
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "ratio_of_preemptive_abort_to_total_abort:\t" << rate
                  << std::endl;
    }
}

void Result::displayReadLatencyRate(size_t clocks_per_us, size_t extime,
                                    size_t thread_num) const {
    if (total_read_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_read_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "read_latency_rate:\t" << rate << std::endl;
    }
}

void Result::displayRtsupdRate() const {
    if (total_rtsupd_chances_ != 0U) {
        long double rate = static_cast<double>(total_rtsupd_) /
                           (static_cast<double>(total_rtsupd_) +
                            static_cast<double>(total_rtsupd_chances_));
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4) << "rtsupd_rate:\t"
                  << rate << std::endl;
    }
}

void Result::displayTemperatureResets() const {
    if (total_temperature_resets_ != 0U) {
        std::cout << "temperature_resets:\t" << total_temperature_resets_
                  << std::endl;
    }
}

void Result::displayTimestampHistorySuccessCounts() const {
    if (total_timestamp_history_success_counts_ != 0U) {
        std::cout << "timestamp_history_success_counts:\t"
                  << total_timestamp_history_success_counts_ << std::endl;
    }
}

void Result::displayTimestampHistoryFailCounts() const {
    if (total_timestamp_history_fail_counts_ != 0U) {
        std::cout << "timestamp_history_fail_counts:\t"
                  << total_timestamp_history_fail_counts_ << std::endl;
    }
}

void Result::displayTreeTraversal() const {
    if (total_tree_traversal_ != 0U) {
        std::cout << "tree_traversal:\t" << total_tree_traversal_ << std::endl;
    }
}

void Result::displayTMTElementMalloc() const {
    if (total_TMT_element_malloc_ != 0U) {
        std::cout << "TMT_element_malloc:\t" << total_TMT_element_malloc_
                  << std::endl;
    }
}

void Result::displayTMTElementReuse() const {
    if (total_TMT_element_reuse_ != 0U) {
        std::cout << "TMT_element_reuse:\t" << total_TMT_element_reuse_
                  << std::endl;
    }
}

void Result::displayValiLatencyRate(size_t clocks_per_us, size_t extime,
                                    size_t thread_num) const {
    if (total_vali_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_vali_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "vali_latency_rate:\t" << rate << std::endl;
    }
}

void Result::displayValidationFailureByTidRate() const {
    if (total_validation_failure_by_tid_ != 0U) {
        long double rate =
                static_cast<double>(total_validation_failure_by_tid_) /
                static_cast<double>(total_abort_by_validation_);
        std::cout << "validation_failure_by_tid:\t"
                  << total_validation_failure_by_tid_ << std::endl;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "validation_failure_by_tid_rate:\t" << rate << std::endl;
    }
}

void Result::displayValidationFailureByWritelockRate() const {
    if (total_validation_failure_by_writelock_ != 0U) {
        long double rate =
                static_cast<double>(total_validation_failure_by_writelock_) /
                static_cast<double>(total_abort_by_validation_);
        std::cout << "validation_failure_by_writelock:\t"
                  << total_validation_failure_by_writelock_ << std::endl;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "validation_failure_by_writelock_rate:\t" << rate
                  << std::endl;
    }
}

void Result::displayVersionMalloc() const {
    if (total_version_malloc_ != 0U) {
        std::cout << "version_malloc:\t" << total_version_malloc_ << std::endl;
    }
}

void Result::displayVersionReuse() const {
    if (total_version_reuse_ != 0U) {
        std::cout << "version_reuse:\t" << total_version_reuse_ << std::endl;
    }
}

void Result::displayWriteLatencyRate(size_t clocks_per_us, size_t extime,
                                     size_t thread_num) const {
    if (total_write_latency_ != 0U) {
        constexpr std::size_t ten = 10;
        constexpr std::size_t six = 6;
        long double rate = static_cast<long double>(total_write_latency_) /
                           (static_cast<long double>(clocks_per_us) *
                            powl(ten, six) * static_cast<long double>(extime)) /
                           thread_num;
        FmtHolder fmt{std::cout};
        std::cout << std::fixed << std::setprecision(4)
                  << "write_latency_rate:\t" << rate << std::endl;
    }
}

void Result::addLocalAbortCounts(uint64_t count) {
    total_abort_counts_ += count;
}

void Result::addLocalCommitCounts(uint64_t count) {
    total_commit_counts_ += count;
}

void Result::addLocalAbortByOperation(uint64_t count) {
    total_abort_by_operation_ += count;
}

void Result::addLocalAbortByValidation(uint64_t count) {
    total_abort_by_validation_ += count;
}

void Result::addLocalCommitLatency(uint64_t count) {
    total_commit_latency_ += count;
}

void Result::addLocalBackoffLatency(uint64_t count) {
    total_backoff_latency_ += count;
}

void Result::addLocalEarlyAborts(uint64_t count) {
    total_early_aborts_ += count;
}

void Result::addLocalExtraReads(uint64_t count) { total_extra_reads_ += count; }

void Result::addLocalGCCounts(uint64_t count) { total_gc_counts_ += count; }

void Result::addLocalGCVersionCounts(uint64_t count) {
    total_gc_version_counts_ += count;
}

void Result::addLocalGCTMTElementsCounts(uint64_t count) {
    total_gc_TMT_elements_counts_ += count;
}

void Result::addLocalGCLatency(uint64_t count) { total_gc_latency_ += count; }

void Result::addLocalMakeProcedureLatency(uint64_t count) {
    total_make_procedure_latency_ += count;
}

void Result::addLocalMemcpys(uint64_t count) { total_memcpys += count; }

void Result::addLocalPreemptiveAbortsCounts(uint64_t count) {
    total_preemptive_aborts_counts_ += count;
}

void Result::addLocalReadLatency(uint64_t count) {
    total_read_latency_ += count;
}

void Result::addLocalRtsupd(uint64_t count) { total_rtsupd_ += count; }

void Result::addLocalRtsupdChances(uint64_t count) {
    total_rtsupd_chances_ += count;
}

void Result::addLocalTemperatureResets(uint64_t count) {
    total_temperature_resets_ += count;
}

void Result::addLocalTMTElementsMalloc(uint64_t count) {
    total_TMT_element_malloc_ += count;
}

void Result::addLocalTMTElementsReuse(uint64_t count) {
    total_TMT_element_reuse_ += count;
}

void Result::addLocalTimestampHistoryFailCounts(uint64_t count) {
    total_timestamp_history_fail_counts_ += count;
}

void Result::addLocalTimestampHistorySuccessCounts(uint64_t count) {
    total_timestamp_history_success_counts_ += count;
}

void Result::addLocalTreeTraversal(uint64_t count) {
    total_tree_traversal_ += count;
}

void Result::addLocalValiLatency(uint64_t count) {
    total_vali_latency_ += count;
}

void Result::addLocalValidationFailureByTid(uint64_t count) {
    total_validation_failure_by_tid_ += count;
}

void Result::addLocalValidationFailureByWritelock(uint64_t count) {
    total_validation_failure_by_writelock_ += count;
}

void Result::addLocalVersionMalloc(uint64_t count) {
    total_version_malloc_ += count;
}

void Result::addLocalVersionReuse(uint64_t count) {
    total_version_reuse_ += count;
}

void Result::addLocalWriteLatency(uint64_t count) {
    total_write_latency_ += count;
}

void Result::displayAllResult([[maybe_unused]] size_t clocks_per_us,
                              size_t extime,
                              [[maybe_unused]] size_t thread_num) const {
    displayAbortByOperationRate();
    displayAbortByValidationRate();
    displayCommitLatencyRate(clocks_per_us, extime, thread_num);
    displayBackoffLatencyRate(clocks_per_us, extime, thread_num);
    displayEarlyAbortRate();
    displayExtraReads();
    displayGCCounts();
    displayGCLatencyRate(clocks_per_us, extime, thread_num);
    displayGCTMTElementsCounts();
    displayGCVersionCounts();
    displayMakeProcedureLatencyRate(clocks_per_us, extime, thread_num);
    displayMemcpys();
    displayOtherWorkLatencyRate(clocks_per_us, extime, thread_num);
    displayPreemptiveAbortsCounts();
    displayRatioOfPreemptiveAbortToTotalAbort();
    displayReadLatencyRate(clocks_per_us, extime, thread_num);
    displayRtsupdRate();
    displayTemperatureResets();
    displayTimestampHistorySuccessCounts();
    displayTimestampHistoryFailCounts();
    displayTMTElementMalloc();
    displayTMTElementReuse();
    displayTreeTraversal();
    displayWriteLatencyRate(clocks_per_us, extime, thread_num);
    displayValiLatencyRate(clocks_per_us, extime, thread_num);
    displayValidationFailureByTidRate();
    displayValidationFailureByWritelockRate();
    displayVersionMalloc();
    displayVersionReuse();
    displayAbortCounts();
    displayCommitCounts();
    displayRusageRUMaxrss();
    displayAbortRate();
    displayTps(extime);
    displayEnvironmentalParameter();
}

void Result::addLocalAllResult(const Result& other) {
    addLocalAbortCounts(other.local_abort_counts_);
    addLocalCommitCounts(other.local_commit_counts_);
    addLocalAbortByOperation(other.local_abort_by_operation_);
    addLocalAbortByValidation(other.local_abort_by_validation_);
    addLocalBackoffLatency(other.local_backoff_latency_);
    addLocalCommitLatency(other.local_commit_latency_);
    addLocalEarlyAborts(other.local_early_aborts_);
    addLocalExtraReads(other.local_extra_reads_);
    addLocalGCCounts(other.local_gc_counts_);
    addLocalGCLatency(other.local_gc_latency_);
    addLocalGCVersionCounts(other.local_gc_version_counts_);
    addLocalGCTMTElementsCounts(other.local_gc_TMT_elements_counts_);
    addLocalMakeProcedureLatency(other.local_make_procedure_latency_);
    addLocalMemcpys(other.local_memcpys);
    addLocalPreemptiveAbortsCounts(other.local_preemptive_aborts_counts_);
    addLocalReadLatency(other.local_read_latency_);
    addLocalRtsupd(other.local_rtsupd_);
    addLocalRtsupdChances(other.local_rtsupd_chances_);
    addLocalTimestampHistorySuccessCounts(
            other.local_timestamp_history_success_counts_);
    addLocalTimestampHistoryFailCounts(
            other.local_timestamp_history_fail_counts_);
    addLocalTemperatureResets(other.local_temperature_resets_);
    addLocalTreeTraversal(other.local_tree_traversal_);
    addLocalTMTElementsMalloc(other.local_TMT_element_malloc_);
    addLocalTMTElementsReuse(other.local_TMT_element_reuse_);
    addLocalWriteLatency(other.local_write_latency_);
    addLocalValiLatency(other.local_vali_latency_);
    addLocalValidationFailureByTid(other.local_validation_failure_by_tid_);
    addLocalValidationFailureByWritelock(
            other.local_validation_failure_by_writelock_);
    addLocalVersionMalloc(other.local_version_malloc_);
    addLocalVersionReuse(other.local_version_reuse_);
}

} // namespace shirakami
