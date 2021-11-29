set xlabel "# Threads"

set key horiz outside center top box

set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color
set ylabel "Throughput [tps]"
if (exist("tname")) {
  set title tname
}
set output "ycsb_r50_occ_bt1.pdf"
plot \
       "data_r50_v8_wpr0_bt1.dat"    using 1:2:3:4 w    errorlines title   "OCC-v8-wpr0", \
       "data_r50_v64_wpr0_bt1.dat"   using 1:2:3:4 w    errorlines title   "OCC-v64-wpr0", \
       "data_r50_v8_wpr10_bt1.dat"   using 1:2:3:4 w    errorlines title   "OCC-v8-wpr10", \
       "data_r50_v64_wpr10_bt1.dat"  using 1:2:3:4 w    errorlines title   "OCC-v64-wpr10", \
       "data_r50_v8_wpr50_bt1.dat"   using 1:2:3:4 w    errorlines title   "OCC-v8-wpr50", \
       "data_r50_v64_wpr50_bt1.dat"  using 1:2:3:4 w    errorlines title   "OCC-v64-wpr50", \

set output "ycsb_r50_occ_bt81.pdf"
plot \
       "data_r50_v8_wpr0_bt81.dat"    using 1:2:3:4 w    errorlines title   "OCC-v8-wpr0", \
       "data_r50_v64_wpr0_bt81.dat"   using 1:2:3:4 w    errorlines title   "OCC-v64-wpr0", \
       "data_r50_v8_wpr10_bt81.dat"   using 1:2:3:4 w    errorlines title   "OCC-v8-wpr10", \
       "data_r50_v64_wpr10_bt81.dat"  using 1:2:3:4 w    errorlines title   "OCC-v64-wpr10", \
       "data_r50_v8_wpr50_bt81.dat"   using 1:2:3:4 w    errorlines title   "OCC-v8-wpr50", \
       "data_r50_v64_wpr50_bt81.dat"  using 1:2:3:4 w    errorlines title   "OCC-v64-wpr50", \

set output "ycsb_batch_wpr0.pdf"
plot \
       "data_r50_v8_wpr0_bt1.dat"    using 1:9:10:11 w    errorlines title   "bt1", \
       "data_r50_v8_wpr0_bt3.dat"    using 1:9:10:11 w    errorlines title   "bt3", \
       "data_r50_v8_wpr0_bt9.dat"    using 1:9:10:11 w    errorlines title   "bt9", \
       "data_r50_v8_wpr0_bt27.dat"   using 1:9:10:11 w    errorlines title   "bt27", \
       "data_r50_v8_wpr0_bt81.dat"   using 1:9:10:11 w    errorlines title   "bt81", \

set output "ycsb_batch_wpr50.pdf"
plot \
       "data_r50_v8_wpr50_bt1.dat"    using 1:9:10:11 w    errorlines title   "bt1", \
       "data_r50_v8_wpr50_bt3.dat"    using 1:9:10:11 w    errorlines title   "bt3", \
       "data_r50_v8_wpr50_bt9.dat"    using 1:9:10:11 w    errorlines title   "bt9", \
       "data_r50_v8_wpr50_bt27.dat"   using 1:9:10:11 w    errorlines title   "bt27", \
       "data_r50_v8_wpr50_bt81.dat"   using 1:9:10:11 w    errorlines title   "bt81", \

##################################
#abort rate
#set format y "%1.1f"
#set yrange [0:1]
#set ylabel "Abort Rate"
#
#set output "ycsb_r50_ar.pdf"
#plot \
#       "data_r50_v8_wpr0.dat"   using 1:5:6:7 w     errorlines title   "OCC-v8-wpr0", \
#       "data_r50_v8_wpr0.dat"   using 1:10:11:12 w  errorlines title   "BATCH-v8-wpr0", \
#       "data_r50_v64_wpr0.dat"  using 1:5:6:7 w     errorlines title  "OCC-v64-wpr0", \
#       "data_r50_v64_wpr0.dat"  using 1:10:11:12 w  errorlines title "BATCH-v64-wpr0", \
#       "data_r50_v8_wpr10.dat"  using 1:5:6:7 w     errorlines title   "OCC-v8-wpr10", \
#       "data_r50_v8_wpr10.dat"  using 1:10:11:12 w  errorlines title  "BATCH-v8-wpr10", \
#       "data_r50_v64_wpr10.dat" using 1:5:6:7 w     errorlines title  "OCC-v64-wpr10", \
#       "data_r50_v64_wpr10.dat" using 1:10:11:12 w  errorlines title "BATCH-v64-wpr10", \
#       "data_r50_v8_wpr50.dat"  using 1:5:6:7 w     errorlines title   "OCC-v8-wpr50", \
#       "data_r50_v8_wpr50.dat"  using 1:10:11:12 w  errorlines title  "BATCH-v8-wpr50", \
#       "data_r50_v64_wpr50.dat" using 1:5:6:7 w     errorlines title  "OCC-v64-wpr50", \
#       "data_r50_v64_wpr50.dat" using 1:10:11:12 w  errorlines title "BATCH-v64-wpr50", \
#
#set output "ycsb_r80_ar.pdf"
#plot \
#       "data_r80_v8_wpr0.dat"   using 1:5:6:7 w      errorlines title   "OCC-v8-wpr0", \
#       "data_r80_v8_wpr0.dat"   using 1:10:11:12 w   errorlines title   "BATCH-v8-wpr0", \
#       "data_r80_v64_wpr0.dat"  using 1:5:6:7 w      errorlines title  "OCC-v64-wpr0", \
#       "data_r80_v64_wpr0.dat"  using 1:10:11:12 w   errorlines title "BATCH-v64-wpr0", \
#       "data_r80_v8_wpr10.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr10", \
#       "data_r80_v8_wpr10.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr10", \
#       "data_r80_v64_wpr10.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr10", \
#       "data_r80_v64_wpr10.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr10", \
#       "data_r80_v8_wpr50.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr50", \
#       "data_r80_v8_wpr50.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr50", \
#       "data_r80_v64_wpr50.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr50", \
#       "data_r80_v64_wpr50.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr50", \
#
#set output "ycsb_r99_ar.pdf"
#plot \
#       "data_r99_v8_wpr0.dat"   using 1:5:6:7 w      errorlines title   "OCC-v8-wpr0", \
#       "data_r99_v8_wpr0.dat"   using 1:10:11:12 w   errorlines title   "BATCH-v8-wpr0", \
#       "data_r99_v64_wpr0.dat"  using 1:5:6:7 w      errorlines title  "OCC-v64-wpr0", \
#       "data_r99_v64_wpr0.dat"  using 1:10:11:12 w   errorlines title "BATCH-v64-wpr0", \
#       "data_r99_v8_wpr10.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr10", \
#       "data_r99_v8_wpr10.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr10", \
#       "data_r99_v64_wpr10.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr10", \
#       "data_r99_v64_wpr10.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr10", \
#       "data_r99_v8_wpr50.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr50", \
#       "data_r99_v8_wpr50.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr50", \
#       "data_r99_v64_wpr50.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr50", \
#       "data_r99_v64_wpr50.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr50", \
#
###################################
##cache miss rate
#set format y "%.0f"
#set yrange [0:100]
#set ylabel "Cache Miss Rate"
#
#set output "ycsb_r50_cr.pdf"
#plot \
#       "data_r50_v8_wpr0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpr0", \
#       "data_r50_v64_wpr0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpr0", \
#       "data_r50_v8_wpr10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr10", \
#       "data_r50_v64_wpr10.dat" using 1:14:15:16 w  errorlines title  "v64-wpr10", \
#       "data_r50_v8_wpr50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr50", \
#       "data_r50_v64_wpr50.dat" using 1:14:15:16 w  errorlines title  "v64-wpr50", \
#
#set output "ycsb_r80_cr.pdf"
#plot \
#       "data_r80_v8_wpr0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpr0", \
#       "data_r80_v64_wpr0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpr0", \
#       "data_r80_v8_wpr10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr10", \
#       "data_r80_v64_wpr10.dat" using 1:14:15:16 w  errorlines title  "v64-wpr10", \
#       "data_r80_v8_wpr50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr50", \
#       "data_r80_v64_wpr50.dat" using 1:14:15:16 w  errorlines title  "v64-wpr50", \
#
#
#set output "ycsb_r99_cr.pdf"
#plot \
#       "data_r99_v8_wpr0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpr0", \
#       "data_r99_v64_wpr0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpr0", \
#       "data_r99_v8_wpr10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr10", \
#       "data_r99_v64_wpr10.dat" using 1:14:15:16 w  errorlines title  "v64-wpr10", \
#       "data_r99_v8_wpr50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpr50", \
#       "data_r99_v64_wpr50.dat" using 1:14:15:16 w  errorlines title  "v64-wpr50", \
#
#
