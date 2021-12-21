set xlabel "Conflict Rate [%]"

set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color
set ylabel "OL Throughput [tps]"
set title "bcc5"
set output "bcc_5.pdf"
plot \
       "bcc_5.dat"   using 1:2:3:4 w errorlines notitle, \
