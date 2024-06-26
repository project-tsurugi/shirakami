cpumhz=2100
duration=10
key_length=64
ops=10
record=1000000
rratioary=(99 80 50)
#rratioary=(50)
skew=0
thread=224
#val_length_ary=(8)
val_length_ary=(8 64 1024)
epoch=5

for val_length in "${val_length_ary[@]}"
do
  for rratio in "${rratioary[@]}"
  do
    if test $rratio = 99 ; then
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
    elif test $rratio = 80 ; then
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
    elif test $rratio = 50 ; then
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

    echo "#tuple num, avg-tps, min-tps, max-tps, avg-ar, min-ar, max-ar, avg-camiss, min-camiss, max-camiss" >> $result
    echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -ops $ops -record $record -rratio $rratio -skew $skew -thread $thread -val_length $val_length" >> $result

    for ((thread=28; thread<=112; thread+=28))
    do
      echo "sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -ops $ops -record $record -rratio $rratio -skew $skew -thread $thread -val_length $val_length"
      echo "Thread number $thread"

      sumTH=0
      sumAR=0
      sumCA=0
      maxTH=0
      maxAR=0
      maxCA=0
      minTH=0
      minAR=0
      minCA=0
      for ((i = 1; i <= epoch; ++i))
      do
        if test $host = $dbs11 ; then
      date
          sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bench/ycsb -cpumhz $cpumhz -duration $duration -key_length $key_length -ops $ops -record $record -rratio $rratio -skew $skew -thread $thread -val_length $val_length > exp.txt
        fi

        tmpTH=`grep throughput ./exp.txt | awk '{print $2}'`
        tmpAR=`grep abort_rate ./exp.txt | awk '{print $2}'`
        tmpCA=`grep cache-misses ./ana.txt | awk '{print $4}'`
        sumTH=`echo "$sumTH + $tmpTH" | bc`
        sumAR=`echo "scale=4; $sumAR + $tmpAR" | bc | xargs printf %.4f`
        sumCA=`echo "$sumCA + $tmpCA" | bc`
        echo "tmpTH: $tmpTH, tmpAR: $tmpAR, tmpCA: $tmpCA"

        if test $i -eq 1 ; then
          maxTH=$tmpTH
          maxAR=$tmpAR
          maxCA=$tmpCA
          minTH=$tmpTH
          minAR=$tmpAR
          minCA=$tmpCA
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
      done
      avgTH=`echo "$sumTH / $epoch" | bc`
      avgAR=`echo "scale=4; $sumAR / $epoch" | bc | xargs printf %.4f`
      avgCA=`echo "$sumCA / $epoch" | bc`
      echo "sumTH: $sumTH, sumAR: $sumAR, sumCA: $sumCA"
      echo "avgTH: $avgTH, avgAR: $avgAR, avgCA: $avgCA"
      echo "maxTH: $maxTH, maxAR: $maxAR, maxCA: $maxCA"
      echo "minTH: $minTH, minAR: $minAR, minCA: $minCA"
      echo ""
      echo "$thread $avgTH $minTH $maxTH $avgAR $minAR $maxAR $avgCA $minCA $maxCA" >> $result
    done
  done
done
