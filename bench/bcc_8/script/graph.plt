set format y "%1.1t{/Symbol \264}10^{%T}"
set format y2 "%1.1t{/Symbol \264}10^{%T}"

set grid

set terminal pdfcairo enhanced color

set ylabel "throughput[tps]"
set y2label "maxrss[kb]"

set y2tics

if (exist("tname")) {
       set title tname
       if (tname eq "vt") {
              set output "bcc_8_vec_txs.pdf"
              set xlabel "tx size"
              set logscale x
              set format x "%1.1t{/Symbol \264}10^{%T}"
              set autoscale x
              plot \
              "bcc_8.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
              "bcc_8.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
       } else {
                     if (tname eq "vs") {
                            set output "bcc_8_vec_skew.pdf"
                            set xlabel "skew"
                            set nologscale x
                            set format x "%3.2f"
                            set xrange [0:1]
                            plot \
                            "bcc_8.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
                            "bcc_8.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
                     } else {
                            if (tname eq "ut") {
                                   set output "bcc_8_um_txs.pdf"
                                   set xlabel "tx size"
                                   set logscale x
                                   set format x "%1.1t{/Symbol \264}10^{%T}"
                                   set autoscale x
                                   plot \
                                   "bcc_8.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
                                   "bcc_8.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
                            } else {
                                   set output "bcc_8_um_skew.pdf"
                                   set xlabel "skew"
                                   set nologscale x
                                   set format x "%3.2f"
                                   set xrange [0:1]
                                   plot \
                                   "bcc_8.dat"   u 1:2:3:4 w errorlines axes x1y1 title "throughput", \
                                   "bcc_8.dat"   u 1:5:6:7 w errorlines axes x1y2 title "maxrss", \
                            }
                     }
       }
}



