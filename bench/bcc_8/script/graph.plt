set xlabel "# recs"
set logscale x
set format x "%1.1t{/Symbol \264}10^{%T}"
set format y "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color

set ylabel "throughput[tps]"
if (exist("tname")) {
       set title tname
       if (tname eq "pure") {
              set output "bcc_7_pure.pdf"
       } else {
              set output "bcc_7_ra.pdf"
       }
}

plot \
       "bcc_7.dat"   using 1:2:3:4 w errorlines notitle, \


