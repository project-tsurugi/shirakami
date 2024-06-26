cpumhz=2100
duration=1
record=100
key_length=64
rratioary=(99 80 50)
skew=0
thread=1
val_length_ary=(8 64 1024)
include_long_tx=true
long_tx_ops=1
epoch=5

for val_length in "${val_length_ary[@]}"
do
  for long_tx_rratio in "${rratioary[@]}"
  do
    if test $long_tx_rratio = 99 ; then
        if test $val_length = 64 ; then
          result=result_ycsb_r99_k64_v64.dat
        elif test $val_length = 8 ; then
          result=result_ycsb_r99_k64_v8.dat
        elif test $val_length = 1024 ; then
          result=result_ycsb_r99_k64_v1024.dat
        elif test $val_length = 16384 ; then
          result=result_ycsb_r99_k64_v16384.dat
        else
            echo "BUG"
            exit 1
        fi
    elif test $long_tx_rratio = 80 ; then
        if test $val_length = 64 ; then
          result=result_ycsb_r80_k64_v64.dat
        elif test $val_length = 8 ; then
          result=result_ycsb_r80_k64_v8.dat
        elif test $val_length = 1024 ; then
          result=result_ycsb_r80_k64_v1024.dat
        elif test $val_length = 16384 ; then
          result=result_ycsb_r80_k64_v16384.dat
        else
            echo "BUG"
            exit 1
        fi
    elif test $long_tx_rratio = 50 ; then
        if test $val_length = 64 ; then
          result=result_ycsb_r50_k64_v64.dat
        elif test $val_length = 8 ; then
          result=result_ycsb_r50_k64_v8.dat
        elif test $val_length = 1024 ; then
          result=result_ycsb_r50_k64_v1024.dat
        elif test $val_length = 16384 ; then
          result=result_ycsb_r50_k64_v16384.dat
        else
            echo "BUG"
            exit 1
        fi
    else
      echo "BUG"
      exit 1
    fi
    rm $result

    echo "#ops num, avg-cops, min-cops, max-cops, avg-ar, min-ar, max-ar, avg-camiss, min-camiss, max-camiss, avg-maxrss, min-maxrss, max-maxrss, avg-cpr-global-version, min-cpr-global-version, max-cpr-global-version" >> $result
    echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -record $record -skew $skew -thread $thread -val_length $val_length -include_long_tx $include_long_tx -long_tx_ops $long_tx_ops -long_tx_rratio $long_tx_rratio" >> $result

    for ((long_tx_ops=1; long_tx_ops<=10000; long_tx_ops*=10))
    do
      echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -record $record -skew $skew -thread $thread -val_length $val_length -include_long_tx $include_long_tx -long_tx_ops $long_tx_ops -long_tx_rratio $long_tx_rratio"
      echo "Ops $long_tx_ops"

      sumTH=0
      sumAR=0
      sumCA=0
      sumMAXRSS=0
      sumCGV=0
      maxTH=0
      maxAR=0
      maxCA=0
      maxMAXRSS=0
      maxCGV=0
      minTH=0
      minAR=0
      minCA=0
      minMAXRSS=0
      minCGV=0
      for ((i = 1; i <= epoch; ++i))
      do
        date
        sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -record $record -skew $skew -thread $thread -val_length $val_length -include_long_tx $include_long_tx -long_tx_ops $long_tx_ops -long_tx_rratio $long_tx_rratio > exp.txt

        tmpTH=`grep throughput ./exp.txt | grep -v long | awk '{print $2}'`
        tmpAR=`grep abort_rate ./exp.txt | grep -v long | awk '{print $2}'`
        tmpCA=`grep cache-misses ./ana.txt | awk '{print $4}'`
        tmpMAXRSS=`grep maxrss ./exp.txt | awk '{print $2}'`
        tmpCGV=`grep cpr_global_version ./exp.txt | awk '{print $2}'`
        sumTH=`echo "scale=4; $sumTH + $tmpTH" | bc | xargs printf %10.4f`
        sumAR=`echo "scale=4; $sumAR + $tmpAR" | bc | xargs printf %.4f`
        sumCA=`echo "$sumCA + $tmpCA" | bc`
        sumMAXRSS=`echo "$sumMAXRSS + $tmpMAXRSS" | bc`
        sumCGV=`echo "$sumCGV + $tmpCGV" | bc`
        echo "tmpTH: $tmpTH, tmpAR: $tmpAR, tmpCA: $tmpCA"

        if test $i -eq 1 ; then
          maxTH=$tmpTH
          maxAR=$tmpAR
          maxCA=$tmpCA
          maxMAXRSS=$tmpMAXRSS
          maxCGV=$tmpCGV
          minTH=$tmpTH
          minAR=$tmpAR
          minCA=$tmpCA
          minMAXRSS=$tmpMAXRSS
          minCGV=$tmpCGV
        fi

        flag=`echo "$tmpTH > $maxTH" | bc`
        if test $flag -eq 1 ; then
          maxTH=$tmpTH
        fi
        flag=`echo "$tmpAR > $maxAR" | bc`
        if test $flag -eq 1 ; then
          maxAR=$tmpAR
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


        flag=`echo "$tmpTH < $minTH" | bc`
        if test $flag -eq 1 ; then
          minTH=$tmpTH
        fi
        flag=`echo "$tmpAR < $minAR" | bc`
        if test $flag -eq 1 ; then
          minAR=$tmpAR
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
      avgTH=`echo "scale=4; $sumTH / $epoch" | bc | xargs printf %10.4f`
      avgAR=`echo "scale=4; $sumAR / $epoch" | bc | xargs printf %.4f`
      avgCA=`echo "$sumCA / $epoch" | bc`
      avgMAXRSS=`echo "$sumMAXRSS / $epoch" | bc`
      avgCGV=`echo "$sumCGV / $epoch" | bc`
      echo "sumTH: $sumTH, sumAR: $sumAR, sumCA: $sumCA, sumMAXRSS: $sumMAXRSS, sumCGV: $sumCGV"
      echo "avgTH: $avgTH, avgAR: $avgAR, avgCA: $avgCA, avgMAXRSS: $avgMAXRSS, avgCGV: $avgCGV"
      echo "maxTH: $maxTH, maxAR: $maxAR, maxCA: $maxCA, maxMAXRSS: $maxMAXRSS, maxCGV: $maxCGV"
      echo "minTH: $minTH, minAR: $minAR, minCA: $minCA, minMAXRSS: $minMAXRSS, minCGV: $minCGV"
      echo ""
      avgTH=`echo "$avgTH * $long_tx_ops" | bc`
      minTH=`echo "$minTH * $long_tx_ops" | bc`
      maxTH=`echo "$maxTH * $long_tx_ops" | bc`
      echo "$long_tx_ops $avgTH $minTH $maxTH $avgAR $minAR $maxAR $avgCA $minCA $maxCA $avgMAXRSS $minMAXRSS $maxMAXRSS $avgCGV $minCGV $maxCGV" >> $result
    done
  done
done
