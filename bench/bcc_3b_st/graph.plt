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
       "data_r50_v8_wpw0_bt1.dat"   using 1:2:3:4 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt1.dat"   using 1:2:3:4 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt1.dat"   using 1:2:3:4 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt1.dat"   using 1:2:3:4 w    errorlines title   "wpw3", \

set output "ycsb_r50_occ_bt16.pdf"
plot \
       "data_r50_v8_wpw0_bt16.dat"   using 1:2:3:4 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt16.dat"   using 1:2:3:4 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt16.dat"   using 1:2:3:4 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt16.dat"   using 1:2:3:4 w    errorlines title   "wpw3", \

set output "ycsb_r50_bt_bt1.pdf"
plot \
       "data_r50_v8_wpw0_bt1.dat"   using 1:9:10:11 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt1.dat"   using 1:9:10:11 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt1.dat"   using 1:9:10:11 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt1.dat"   using 1:9:10:11 w    errorlines title   "wpw3", \

set output "ycsb_r50_bt_bt16.pdf"
plot \
       "data_r50_v8_wpw0_bt16.dat"   using 1:9:10:11 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt16.dat"   using 1:9:10:11 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt16.dat"   using 1:9:10:11 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt16.dat"   using 1:9:10:11 w    errorlines title   "wpw3", \

##################################
#abort rate
set format y "%1.1f"
set yrange [0:1]
set ylabel "Abort Rate"

set output "ycsb_r50_occ_bt1_ar.pdf"
plot \
       "data_r50_v8_wpw0_bt1.dat"   using 1:5:6:7 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt1.dat"   using 1:5:6:7 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt1.dat"   using 1:5:6:7 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt1.dat"   using 1:5:6:7 w    errorlines title   "wpw3", \

set output "ycsb_r50_occ_bt16_ar.pdf"
plot \
       "data_r50_v8_wpw0_bt16.dat"   using 1:5:6:7 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt16.dat"   using 1:5:6:7 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt16.dat"   using 1:5:6:7 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt16.dat"   using 1:5:6:7 w    errorlines title   "wpw3", \

set output "ycsb_r50_bt_bt1_ar.pdf"
plot \
       "data_r50_v8_wpw0_bt1.dat"   using 1:12:13:14 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt1.dat"   using 1:12:13:14 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt1.dat"   using 1:12:13:14 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt1.dat"   using 1:12:13:14 w    errorlines title   "wpw3", \

set output "ycsb_r50_bt_bt16_ar.pdf"
plot \
       "data_r50_v8_wpw0_bt16.dat"   using 1:12:13:14 w    errorlines title   "wpw0", \
       "data_r50_v8_wpw1_bt16.dat"   using 1:12:13:14 w    errorlines title   "wpw1", \
       "data_r50_v8_wpw2_bt16.dat"   using 1:12:13:14 w    errorlines title   "wpw2", \
       "data_r50_v8_wpw3_bt16.dat"   using 1:12:13:14 w    errorlines title   "wpw3", \


###################################
##cache miss rate
#set format y "%.0f"
#set yrange [0:100]
#set ylabel "Cache Miss Rate"
#
#set output "ycsb_r50_cr.pdf"
#plot \
#       "data_r50_v8_wpw0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpw0", \
#       "data_r50_v64_wpw0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpw0", \
#       "data_r50_v8_wpw10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw10", \
#       "data_r50_v64_wpw10.dat" using 1:14:15:16 w  errorlines title  "v64-wpw10", \
#       "data_r50_v8_wpw50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw50", \
#       "data_r50_v64_wpw50.dat" using 1:14:15:16 w  errorlines title  "v64-wpw50", \
#
#set output "ycsb_r80_cr.pdf"
#plot \
#       "data_r80_v8_wpw0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpw0", \
#       "data_r80_v64_wpw0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpw0", \
#       "data_r80_v8_wpw10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw10", \
#       "data_r80_v64_wpw10.dat" using 1:14:15:16 w  errorlines title  "v64-wpw10", \
#       "data_r80_v8_wpw50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw50", \
#       "data_r80_v64_wpw50.dat" using 1:14:15:16 w  errorlines title  "v64-wpw50", \
#
#
#set output "ycsb_r99_cr.pdf"
#plot \
#       "data_r99_v8_wpw0.dat"   using 1:14:15:16 w  errorlines title  "v8-wpw0", \
#       "data_r99_v64_wpw0.dat"  using 1:14:15:16 w  errorlines title  "v64-wpw0", \
#       "data_r99_v8_wpw10.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw10", \
#       "data_r99_v64_wpw10.dat" using 1:14:15:16 w  errorlines title  "v64-wpw10", \
#       "data_r99_v8_wpw50.dat"  using 1:14:15:16 w  errorlines title  "v8-wpw50", \
#       "data_r99_v64_wpw50.dat" using 1:14:15:16 w  errorlines title  "v64-wpw50", \
#
#
