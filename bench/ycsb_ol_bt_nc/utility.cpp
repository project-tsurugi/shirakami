
#include <iostream>

#include "declare_gflags.h"

#include "glog/logging.h"

void check_flags() {
    std::cout << "general options" << std::endl;
    if (FLAGS_cpumhz > 1) {
        printf("FLAGS_cpumhz : %zu\n", FLAGS_cpumhz); // NOLINT
    } else {
        LOG(FATAL) << "CPU MHz of execution environment. It is used measuring some time. It must be larger than 0.";
    }
    if (FLAGS_duration >= 1) {
        printf("FLAGS_duration : %zu\n", FLAGS_duration); // NOLINT
    } else {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }
    if (FLAGS_key_len > 0) {
        printf("FLAGS_key_len : %zu\n", FLAGS_key_len); // NOLINT
    }
    if (FLAGS_val_len > 1) {
        printf("FLAGS_val_len : %zu\n", FLAGS_val_len); // NOLINT
    } else {
        LOG(FATAL) << "Length of val must be larger than 0.";
    }

    std::cout << "online options" << std::endl;
    printf("FLAGS_ol_ops : %zu\n", FLAGS_ol_ops);           // NOLINT
    if (FLAGS_ol_rratio >= 0 && FLAGS_ol_rratio <= 100) {   // NOLINT
        printf("FLAGS_ol_rratio : %zu\n", FLAGS_ol_rratio); // NOLINT
    } else {
        LOG(FATAL) << "Rate of reads in a transaction must be in the range 0 to 100.";
    }
    printf("FLAGS_ol_rec : %zu\n", FLAGS_ol_rec); // NOLINT
    if (FLAGS_ol_skew >= 0 && FLAGS_ol_skew < 1) {
        std::cout << "FLAGS_ol_skew:\t" << FLAGS_ol_skew << std::endl;
    } else {
        LOG(FATAL) << "Access skew of transaction must be in the range 0 to 0.999... .";
    }

    std::cout << "batch options" << std::endl;
    printf("FLAGS_bt_ops : %zu\n", FLAGS_bt_ops);           // NOLINT
    if (FLAGS_bt_rratio >= 0 && FLAGS_bt_rratio <= 100) {   // NOLINT
        printf("FLAGS_bt_rratio : %zu\n", FLAGS_bt_rratio); // NOLINT
    } else {
        LOG(FATAL) << "Rate of reads in a transaction must be in the range 0 to 100.";
    }
    printf("FLAGS_bt_rec : %zu\n", FLAGS_bt_rec); // NOLINT
    if (FLAGS_bt_skew >= 0 && FLAGS_bt_skew < 1) {
        std::cout << "FLAGS_bt_skew:\t" << FLAGS_bt_skew << std::endl;
    } else {
        LOG(FATAL) << "Access skew of transaction must be in the range 0 to 0.999... .";
    }

    printf("Fin check_flags()\n"); // NOLINT
}
