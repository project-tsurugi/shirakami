set xlabel "# Threads"

set key horiz outside center top box

set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color
set ylabel "Throughput [tps]"
if (exist("tname")) {
  set title tname
}
set output "ycsb_r50.pdf"
plot \
       "data_r50_v8_wpr0.dat" using 1:2:3:4 w    errorlines title   "OCC-v8-wpr0", \
       "data_r50_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "BATCH-v8-wpr0", \
       "data_r50_v64_wpr0.dat" using 1:2:3:4 w   errorlines title  "OCC-v64-wpr0", \
       "data_r50_v64_wpr0.dat" using 1:8:9:10 w  errorlines title "BATCH-v64-wpr0", \
       "data_r50_v8_wpr10.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr10", \
       "data_r50_v8_wpr10.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr10", \
       "data_r50_v64_wpr10.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr10", \
       "data_r50_v64_wpr10.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr10", \
       "data_r50_v8_wpr50.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr50", \
       "data_r50_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr50", \
       "data_r50_v64_wpr50.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr50", \
       "data_r50_v64_wpr50.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr50", \

set output "ycsb_r80.pdf"
plot \
       "data_r80_v8_wpr0.dat" using 1:2:3:4 w    errorlines title   "OCC-v8-wpr0", \
       "data_r80_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "BATCH-v8-wpr0", \
       "data_r80_v64_wpr0.dat" using 1:2:3:4 w   errorlines title  "OCC-v64-wpr0", \
       "data_r80_v64_wpr0.dat" using 1:8:9:10 w  errorlines title "BATCH-v64-wpr0", \
       "data_r80_v8_wpr10.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr10", \
       "data_r80_v8_wpr10.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr10", \
       "data_r80_v64_wpr10.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr10", \
       "data_r80_v64_wpr10.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr10", \
       "data_r80_v8_wpr50.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr50", \
       "data_r80_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr50", \
       "data_r80_v64_wpr50.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr50", \
       "data_r80_v64_wpr50.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr50", \

set output "ycsb_r99.pdf"
plot \
       "data_r99_v8_wpr0.dat" using 1:2:3:4 w    errorlines title   "OCC-v8-wpr0", \
       "data_r99_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "BATCH-v8-wpr0", \
       "data_r99_v64_wpr0.dat" using 1:2:3:4 w   errorlines title  "OCC-v64-wpr0", \
       "data_r99_v64_wpr0.dat" using 1:8:9:10 w  errorlines title "BATCH-v64-wpr0", \
       "data_r99_v8_wpr10.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr10", \
       "data_r99_v8_wpr10.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr10", \
       "data_r99_v64_wpr10.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr10", \
       "data_r99_v64_wpr10.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr10", \
       "data_r99_v8_wpr50.dat" using 1:2:3:4 w   errorlines title   "OCC-v8-wpr50", \
       "data_r99_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "BATCH-v8-wpr50", \
       "data_r99_v64_wpr50.dat" using 1:2:3:4 w  errorlines title  "OCC-v64-wpr50", \
       "data_r99_v64_wpr50.dat" using 1:8:9:10 w errorlines title "BATCH-v64-wpr50", \

set output "ycsb_batch.pdf"
plot \
       "data_r50_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "r50-v8-wpr0", \
       "data_r50_v64_wpr0.dat" using 1:8:9:10 w  errorlines title   "r50-v64-wpr0", \
       "data_r50_v8_wpr10.dat" using 1:8:9:10 w  errorlines title   "r50-v8-wpr10", \
       "data_r50_v64_wpr10.dat" using 1:8:9:10 w errorlines title   "r50-v64-wpr10", \
       "data_r50_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "r50-v8-wpr50", \
       "data_r50_v64_wpr50.dat" using 1:8:9:10 w errorlines title  "r50-v64-wpr50", \
       "data_r80_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "r80-v8-wpr0", \
       "data_r80_v64_wpr0.dat" using 1:8:9:10 w  errorlines title   "r80-v64-wpr0", \
       "data_r80_v8_wpr10.dat" using 1:8:9:10 w  errorlines title   "r80-v8-wpr10", \
       "data_r80_v64_wpr10.dat" using 1:8:9:10 w errorlines title   "r80-v64-wpr10", \
       "data_r80_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "r80-v8-wpr50", \
       "data_r80_v64_wpr50.dat" using 1:8:9:10 w errorlines title  "r80-v64-wpr50", \
       "data_r99_v8_wpr0.dat" using 1:8:9:10 w   errorlines title   "r99-v8-wpr0", \
       "data_r99_v64_wpr0.dat" using 1:8:9:10 w  errorlines title   "r99-v64-wpr0", \
       "data_r99_v8_wpr10.dat" using 1:8:9:10 w  errorlines title   "r99-v8-wpr10", \
       "data_r99_v64_wpr10.dat" using 1:8:9:10 w errorlines title   "r99-v64-wpr10", \
       "data_r99_v8_wpr50.dat" using 1:8:9:10 w  errorlines title  "r99-v8-wpr50", \
       "data_r99_v64_wpr50.dat" using 1:8:9:10 w errorlines title  "r99-v64-wpr50", \

##################################
#abort rate
set format y "%1.1f"
set yrange [0:1]
set ylabel "Abort Rate"

set output "ycsb_r50_ar.pdf"
plot \
       "data_r50_v8_wpr0.dat"   using 1:5:6:7 w     errorlines title   "OCC-v8-wpr0", \
       "data_r50_v8_wpr0.dat"   using 1:10:11:12 w  errorlines title   "BATCH-v8-wpr0", \
       "data_r50_v64_wpr0.dat"  using 1:5:6:7 w     errorlines title  "OCC-v64-wpr0", \
       "data_r50_v64_wpr0.dat"  using 1:10:11:12 w  errorlines title "BATCH-v64-wpr0", \
       "data_r50_v8_wpr10.dat"  using 1:5:6:7 w     errorlines title   "OCC-v8-wpr10", \
       "data_r50_v8_wpr10.dat"  using 1:10:11:12 w  errorlines title  "BATCH-v8-wpr10", \
       "data_r50_v64_wpr10.dat" using 1:5:6:7 w     errorlines title  "OCC-v64-wpr10", \
       "data_r50_v64_wpr10.dat" using 1:10:11:12 w  errorlines title "BATCH-v64-wpr10", \
       "data_r50_v8_wpr50.dat"  using 1:5:6:7 w     errorlines title   "OCC-v8-wpr50", \
       "data_r50_v8_wpr50.dat"  using 1:10:11:12 w  errorlines title  "BATCH-v8-wpr50", \
       "data_r50_v64_wpr50.dat" using 1:5:6:7 w     errorlines title  "OCC-v64-wpr50", \
       "data_r50_v64_wpr50.dat" using 1:10:11:12 w  errorlines title "BATCH-v64-wpr50", \

set output "ycsb_r80_ar.pdf"
plot \
       "data_r80_v8_wpr0.dat"   using 1:5:6:7 w      errorlines title   "OCC-v8-wpr0", \
       "data_r80_v8_wpr0.dat"   using 1:10:11:12 w   errorlines title   "BATCH-v8-wpr0", \
       "data_r80_v64_wpr0.dat"  using 1:5:6:7 w      errorlines title  "OCC-v64-wpr0", \
       "data_r80_v64_wpr0.dat"  using 1:10:11:12 w   errorlines title "BATCH-v64-wpr0", \
       "data_r80_v8_wpr10.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr10", \
       "data_r80_v8_wpr10.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr10", \
       "data_r80_v64_wpr10.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr10", \
       "data_r80_v64_wpr10.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr10", \
       "data_r80_v8_wpr50.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr50", \
       "data_r80_v8_wpr50.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr50", \
       "data_r80_v64_wpr50.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr50", \
       "data_r80_v64_wpr50.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr50", \

set output "ycsb_r99_ar.pdf"
plot \
       "data_r99_v8_wpr0.dat"   using 1:5:6:7 w      errorlines title   "OCC-v8-wpr0", \
       "data_r99_v8_wpr0.dat"   using 1:10:11:12 w   errorlines title   "BATCH-v8-wpr0", \
       "data_r99_v64_wpr0.dat"  using 1:5:6:7 w      errorlines title  "OCC-v64-wpr0", \
       "data_r99_v64_wpr0.dat"  using 1:10:11:12 w   errorlines title "BATCH-v64-wpr0", \
       "data_r99_v8_wpr10.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr10", \
       "data_r99_v8_wpr10.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr10", \
       "data_r99_v64_wpr10.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr10", \
       "data_r99_v64_wpr10.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr10", \
       "data_r99_v8_wpr50.dat"  using 1:5:6:7 w      errorlines title   "OCC-v8-wpr50", \
       "data_r99_v8_wpr50.dat"  using 1:10:11:12 w   errorlines title  "BATCH-v8-wpr50", \
       "data_r99_v64_wpr50.dat" using 1:5:6:7 w      errorlines title  "OCC-v64-wpr50", \
       "data_r99_v64_wpr50.dat" using 1:10:11:12 w   errorlines title "BATCH-v64-wpr50", \

