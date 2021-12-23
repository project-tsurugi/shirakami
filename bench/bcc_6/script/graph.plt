set xlabel "# threads"

set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color
set ylabel "gc versions"
set title "bcc-6-ver"
set output "bcc_6_ver.pdf"
plot \
       "bcc_6.dat"   using 1:2:3:4 w errorlines notitle, \

set ylabel "throughput[tps]"
set title "bcc-6-throughput"
set output "bcc_6_th.pdf"
plot \
       "bcc_6.dat"   using 1:5:6:7 w errorlines notitle, \


