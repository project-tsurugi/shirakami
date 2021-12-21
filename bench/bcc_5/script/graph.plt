set xlabel "Conflict Rate [%]"

set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color
set ylabel "Throughput [tps]"
set title "bcc4"
set output "bcc_4.pdf"
plot \
       "bcc_4.dat"   using 1:2:3:4 w errorlines notitle, \
