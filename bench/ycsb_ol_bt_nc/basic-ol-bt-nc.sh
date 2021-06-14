cpumhz=2100
duration=1
key_len=8
#val_len_ary=(8)
val_len_ary=(8 64 1024)

rec=100
ol_ops=10
bt_ops=$rec
skew=0

rratioary=(99 80 50)
#rratioary=(50)
thread=112

epoch=1

for val_len in "${val_len_ary[@]}"
do
  for rratio in "${rratioary[@]}"
  do
    if test $rratio = 99 ; then
        if test $val_len = 64 ; then
          result=result_ycsb_mb_nc_r99_k64_v64.dat
        elif test $val_len = 8 ; then
          result=result_ycsb_mb_nc_r99_k64_v8.dat
        elif test $val_len = 1024 ; then
          result=result_ycsb_mb_nc_r99_k64_v1024.dat
        else
            echo "BUG"
            exit 1
        fi
    elif test $rratio = 80 ; then
        if test $val_len = 64 ; then
          result=result_ycsb_mb_nc_r80_k64_v64.dat
        elif test $val_len = 8 ; then
          result=result_ycsb_mb_nc_r80_k64_v8.dat
        elif test $val_len = 1024 ; then
          result=result_ycsb_mb_nc_r80_k64_v1024.dat
        else
            echo "BUG"
            exit 1
        fi
    elif test $rratio = 50 ; then
        if test $val_len = 64 ; then
          result=result_ycsb_mb_nc_r50_k64_v64.dat
        elif test $val_len = 8 ; then
          result=result_ycsb_mb_nc_r50_k64_v8.dat
        elif test $val_len = 1024 ; then
          result=result_ycsb_mb_nc_r50_k64_v1024.dat
        else
            echo "BUG"
            exit 1
        fi
    else
      echo "BUG"
      exit 1
    fi
    rm $result
  
    echo "#ol-thread, avg-tps, min-tps, max-tps, avg-ops, min-ops, max-ops, avg-opsth, min-opsth, max-opsth, avg-ar, min-ar, max-ar, bt-thread, avg-tps, min-tps, max-tps, avg-ops, min-ops, max-ops, avg-opsth, min-opsth, max-opsth, avg-ar, min-ar, max-ar, avg-camiss, min-camiss, max-camiss, avg-maxrss, min-maxrss, max-maxrss, avg-cgv, min-cgv, max-cgv" >> $result
    echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb_ol_bt_nc/ycsb_ol_bt_nc -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -ol_ops $ol_ops -ol_rratio $rratio -ol_rec $rec -ol_skew $skew -ol_thread x -bt_ops $bt_ops -bt_rratio $rratio -bt_rec $rec -bt_skew $skew -bt_thread 112-x" >> $result
    
    for ((thread=28; thread<=84; thread+=28))
    do
        bt_thread=`echo "112 - $thread" | bc`
    echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb_ol_bt_nc/ycsb_ol_bt_nc -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -ol_ops $ol_ops -ol_rratio $rratio -ol_rec $rec -ol_skew $skew -ol_thread $thread -bt_ops $bt_ops -bt_rratio $rratio -bt_rec $rec -bt_skew $skew -bt_thread $bt_thread"
      echo "online thread number $thread"
      
      sumOLTH=0
      sumOLOPS=0
      sumOLOPSTH=0
      sumOLAR=0
      sumBTTH=0
      sumBTOPS=0
      sumBTOPSTH=0
      sumBTAR=0
      sumCA=0
      sumMAXRSS=0
      sumCGV=0
      maxOLTH=0
      maxOLOPS=0
      maxOLOPSTH=0
      maxOLAR=0
      maxBTTH=0
      maxBTOPS=0
      maxBTOPSTH=0
      maxBTAR=0
      maxCA=0
      maxMAXRSS=0
      maxCGV=0
      minOLTH=0 
      minOLOPS=0 
      minOLOPSTH=0 
      minOLAR=0
      minBTTH=0 
      minBTOPS=0 
      minBTOPSTH=0 
      minBTAR=0
      minCA=0
      minMAXRSS=0
      minCGV=0
      for ((i = 1; i <= epoch; ++i))
      do
          date
          sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb_ol_bt_nc/ycsb_ol_bt_nc -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -ol_ops $ol_ops -ol_rratio $rratio -ol_rec $rec -ol_skew $skew -ol_thread $thread -bt_ops $bt_ops -bt_rratio $rratio -bt_rec $rec -bt_skew $skew -bt_thread $bt_thread > exp.txt
        tmpOLTH=`grep ol_throughput ./exp.txt | grep tps | awk '{print $2}'`
        tmpOLOPS=`grep ol_throughput ./exp.txt | grep ops/s | grep -v ops/s/th | awk '{print $2}'`
        tmpOLOPSTH=`grep ol_throughput ./exp.txt | grep ops/s/th | awk '{print $2}'`
        tmpBTTH=`grep bt_throughput ./exp.txt | grep tps | awk '{print $2}'`
        tmpBTOPS=`grep bt_throughput ./exp.txt | grep ops/s | grep -v ops/s/th | awk '{print $2}'`
        tmpBTOPSTH=`grep bt_throughput ./exp.txt | grep ops/s/th | awk '{print $2}'`
        tmpOLAR=`grep ol_abort_rate ./exp.txt | awk '{print $2}'`
        tmpBTAR=`grep bt_abort_rate ./exp.txt | awk '{print $2}'`
        tmpCA=`grep cache-misses ./ana.txt | awk '{print $4}'`
        tmpMAXRSS=`grep maxrss ./exp.txt | awk '{print $2}'`
        tmpCGV=`grep cpr_global_version ./exp.txt | awk '{print $2}'`
        sumOLTH=`echo "scale=4; $sumOLTH + $tmpOLTH" | bc | xargs printf %10.4f`
        sumOLOPS=`echo "scale=4; $sumOLOPS + $tmpOLOPS" | bc | xargs printf %10.4f`
        sumOLOPSTH=`echo "scale=4; $sumOLOPSTH + $tmpOLOPSTH" | bc | xargs printf %10.4f`
        sumBTTH=`echo "scale=4; $sumBTTH + $tmpBTTH" | bc | xargs printf %10.4f`
        sumBTOPS=`echo "scale=4; $sumBTOPS + $tmpBTOPS" | bc | xargs printf %10.4f`
        sumBTOPSTH=`echo "scale=4; $sumBTOPSTH + $tmpBTOPSTH" | bc | xargs printf %10.4f`
        sumOLAR=`echo "scale=4; $sumOLAR + $tmpOLAR" | bc | xargs printf %.4f`
        sumBTAR=`echo "scale=4; $sumBTAR + $tmpBTAR" | bc | xargs printf %.4f`
        sumCA=`echo "$sumCA + $tmpCA" | bc`
        sumMAXRSS=`echo "$sumMAXRSS + $tmpMAXRSS" | bc`
        sumCGV=`echo "$sumCGV + $tmpCGV" | bc`
        echo "tmpOLTH: $tmpOLTH, tmpOLAR: $tmpOLAR, tmpBTTH: $tmpBTTH, tmpBTAR: $tmpBTAR, tmpCA: $tmpCA"
      
        if test $i -eq 1 ; then
          maxOLTH=$tmpOLTH
          maxOLOPS=$tmpOLOPS
          maxOLOPSTH=$tmpOLOPSTH
          maxBTTH=$tmpBTTH
          maxBTOPS=$tmpBTOPS
          maxBTOPSTH=$tmpBTOPSTH
          maxOLAR=$tmpOLAR
          maxBTAR=$tmpBTAR
          maxCA=$tmpCA
          maxMAXRSS=$tmpMAXRSS
          maxCGV=$tmpCGV
          minOLTH=$tmpOLTH
          minOLOPS=$tmpOLOPS
          minOLOPSTH=$tmpOLOPSTH
          minBTTH=$tmpBTTH
          minBTOPS=$tmpBTOPS
          minBTOPSTH=$tmpBTOPSTH
          minOLAR=$tmpOLAR
          minBTAR=$tmpBTAR
          minCA=$tmpCA
          minMAXRSS=$tmpMAXRSS
          minCGV=$tmpCGV
        fi
      
        flag=`echo "$tmpOLTH > $maxOLTH" | bc`
        if test $flag -eq 1 ; then
          maxOLTH=$tmpOLTH
        fi
        flag=`echo "$tmpOLOPS > $maxOLOPS" | bc`
        if test $flag -eq 1 ; then
          maxOLOPS=$tmpOLOPS
        fi
        flag=`echo "$tmpOLOPSTH > $maxOLOPSTH" | bc`
        if test $flag -eq 1 ; then
          maxOLOPSTH=$tmpOLOPSTH
        fi
        flag=`echo "$tmpBTTH > $maxBTTH" | bc`
        if test $flag -eq 1 ; then
          maxBTTH=$tmpBTTH
        fi
        flag=`echo "$tmpBTOPS > $maxBTOPS" | bc`
        if test $flag -eq 1 ; then
          maxBTOPS=$tmpBTOPS
        fi
        flag=`echo "$tmpBTOPSTH > $maxBTOPSTH" | bc`
        if test $flag -eq 1 ; then
          maxBTOPSTH=$tmpBTOPSTH
        fi
        flag=`echo "$tmpOLAR > $maxOLAR" | bc`
        if test $flag -eq 1 ; then
          maxOLAR=$tmpOLAR
        fi
        flag=`echo "$tmpBTAR > $maxBTAR" | bc`
        if test $flag -eq 1 ; then
          maxBTAR=$tmpBTAR
        fi
        flag=`echo "$tmpCA > $maxCA" | bc`
        if test $flag -eq 1 ; then
          maxCA=$tmpCA
        fi
        flag=`echo "$tmpMAXRSS > $maxMAXRSS" | bc`
        if test $flag -eq 1 ; then
          maxMAXRSS=$tmpMAXRSS
        fi
        flag=`echo "$tmpCGV > $maxCGV" | bc`
        if test $flag -eq 1 ; then
          maxCGV=$tmpCGV
        fi
      
        flag=`echo "$tmpOLTH < $minOLTH" | bc`
        if test $flag -eq 1 ; then
          minOLTH=$tmpOLTH
        fi
        flag=`echo "$tmpOLOPS < $minOLOPS" | bc`
        if test $flag -eq 1 ; then
          minOLOPS=$tmpOLOPS
        fi
        flag=`echo "$tmpOLOPSTH < $minOLOPSTH" | bc`
        if test $flag -eq 1 ; then
          minOLOPSTH=$tmpOLOPSTH
        fi
        flag=`echo "$tmpBTTH < $minBTTH" | bc`
        if test $flag -eq 1 ; then
          minBTTH=$tmpBTTH
        fi
        flag=`echo "$tmpBTOPS < $minBTOPS" | bc`
        if test $flag -eq 1 ; then
          minBTOPS=$tmpBTOPS
        fi
        flag=`echo "$tmpBTOPSTH < $minBTOPSTH" | bc`
        if test $flag -eq 1 ; then
          minBTOPSTH=$tmpBTOPSTH
        fi
        flag=`echo "$tmpOLAR < $minOLAR" | bc`
        if test $flag -eq 1 ; then
          minOLAR=$tmpOLAR
        fi
        flag=`echo "$tmpBTAR < $minBTAR" | bc`
        if test $flag -eq 1 ; then
          minBTAR=$tmpBTAR
        fi
        flag=`echo "$tmpCA < $minCA" | bc`
        if test $flag -eq 1 ; then
          minCA=$tmpCA
        fi
        flag=`echo "$tmpMAXRSS < $minMAXRSS" | bc`
        if test $flag -eq 1 ; then
          minMAXRSS=$tmpMAXRSS
        fi
        flag=`echo "$tmpCGV < $minCGV" | bc`
        if test $flag -eq 1 ; then
          minCGV=$tmpCGV
        fi

      done
      avgOLTH=`echo "scale=4; $sumOLTH / $epoch" | bc | xargs printf %10.4f`
      avgOLOPS=`echo "scale=4; $sumOLOPS / $epoch" | bc | xargs printf %10.4f`
      avgOLOPSTH=`echo "scale=4; $sumOLOPSTH / $epoch" | bc | xargs printf %10.4f`
      avgBTTH=`echo "scale=4; $sumBTTH / $epoch" | bc | xargs printf %10.4f`
      avgBTOPS=`echo "scale=4; $sumBTOPS / $epoch" | bc | xargs printf %10.4f`
      avgBTOPSTH=`echo "scale=4; $sumBTOPSTH / $epoch" | bc | xargs printf %10.4f`
      avgOLAR=`echo "scale=4; $sumOLAR / $epoch" | bc | xargs printf %.4f`
      avgBTAR=`echo "scale=4; $sumBTAR / $epoch" | bc | xargs printf %.4f`
      avgCA=`echo "$sumCA / $epoch" | bc`
      avgMAXRSS=`echo "$sumMAXRSS / $epoch" | bc`
      avgCGV=`echo "$sumCGV / $epoch" | bc`
      echo "$thread $avgOLTH $minOLTH $maxOLTH $avgOLOPS $minOLOPS $maxOLOPS $avgOLOPSTH $minOLOPSTH $maxOLOPSTH $avgOLAR $minOLAR $maxOLAR $bt_thread $avgBTTH $minBTTH $maxBTTH $avgBTOPS $minBTOPS $maxBTOPS $avgBTOPSTH $minBTOPSTH $maxBTOPSTH $avgBTAR $minBTAR $maxBTAR $avgCA $minCA $maxCA $avgMAXRSS $minMAXRSS $maxMAXRSS $avgCGV $minCGV $maxCGV" >> $result
    done
  done
done
