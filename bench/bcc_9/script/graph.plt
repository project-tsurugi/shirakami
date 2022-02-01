set format y "%1.1t{/Symbol \264}10^{%T}"
set format y2 "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color

set ylabel "throughput[tps]"
set y2label "maxrss[kb]"

set y2tics

set xlabel "# threads"
set autoscale x

if (exist("tname")) {
       set title tname
       if (tname eq "rcu") {
              set output "bcc_9_rcu.pdf"
              plot \
              "bcc_9.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
              "bcc_9.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
       } else {
              if (tname eq "rw") {
                     set output "bcc_9_rw.pdf"
                     plot \
                     "bcc_9.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
                     "bcc_9.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
              }
       }
}



