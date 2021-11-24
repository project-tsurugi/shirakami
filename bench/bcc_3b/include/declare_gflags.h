/**
 * @file declare_glags.h
 * @details External link declaration for global variables in ycsb_ol_bt_nc.cpp.
 */

#pragma once

#include "gflags/gflags.h"

/**
 * general option
 */
DECLARE_uint64(cpumhz);
DECLARE_uint64(duration);
DECLARE_uint64(key_len);
DECLARE_uint64(rec);
DECLARE_uint64(val_len);

/**
 * about online tx
 */
DECLARE_uint64(ol_ops);
DECLARE_uint64(ol_rratio);
DECLARE_double(ol_skew);
DECLARE_uint64(ol_thread);
DECLARE_uint64(ol_wp_rratio);

/**
 * about batch tx
 */
DECLARE_uint64(bt_ops);
DECLARE_uint64(bt_others_wp_rratio);
DECLARE_uint64(bt_rratio);
DECLARE_double(bt_skew);
DECLARE_uint64(bt_thread);
