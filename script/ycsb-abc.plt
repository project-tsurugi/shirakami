set xlabel "# Threads"


set key horiz outside center top box

set grid

set terminal pdfcairo enhanced color
set ylabel "Throughput [transactions/s]"
set output "ycsb.pdf"
plot \
       "result_ycsbA_tuple1m.dat" using 1:2 w lp title "YCSB-A", \
       "result_ycsbB_tuple1m.dat" using 1:2 w lp title "YCSB-B", \
       "result_ycsbC_tuple1m.dat" using 1:2 w lp title "YCSB-C", \

set ylabel "Abort Rate"
set output "ycsb_ar.pdf"
plot \
       "result_ycsbA_tuple1m.dat" using 1:5 w lp title "YCSB-A", \
       "result_ycsbB_tuple1m.dat" using 1:5 w lp title "YCSB-B", \
       "result_ycsbC_tuple1m.dat" using 1:5 w lp title "YCSB-C", \
