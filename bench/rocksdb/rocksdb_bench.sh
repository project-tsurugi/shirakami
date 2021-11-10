#rocksdb_bench.sh

# default args
bench_type_array=(0)
each_exp_duration=10
each_exp_num=3

set_data_file_name () {
  mkdir -p data
  if test $1 -eq 0 ; then
    result=./data/result_insert.dat
  elif test $1 -eq 1 ; then
    result=./data/result_batch_insert.dat
  elif test $1 -eq 2 ; then
    result=./data/result_batch_insert.dat
  elif test $1 -eq 3 ; then
    result=./data/result_batch_insert.dat
  else
    echo "BUG"
    exit 1
  fi
  if [ -e $result ]; then
      rm $result
  fi
}

set_args () {
    flag=`echo "$# % 2" | bc`
    if test $flag -eq 1 ; then
        if [ $1 = "-help" ] ; then
            output_help
        elif [ $1 = "--help" ] ; then
            output_help
        else
            echo "Invalid use. do ./rocksdb_bench.sh -help"
            exit 1
        fi
    fi
      # todo impl
}

output_help () {
    echo "
    usage : ./rocksdb_bench.sh
    options :
      -duration : default 3 : Experimental time.
      "
      # todo impl
}

for bench_type in "${bench_type_array[@]}"
do
  set_args $*
  set_data_file_name $bench_type

  for ((thread=28; thread<=224; thread+=28))
  do
    sumTH=0
    minTH=0
    maxTH=0
    for ((i = 1; i <= $each_exp_num; ++i))
    do
      LD_PRELOAD=/home/tanabe/.local/lib/libjemalloc.so ./../../build/bench/rocksdb_bench -bench_type $bench_type -duration $each_exp_duration -thread $thread | tee bench_log
      tmpTH=`grep Throughput bench_log | awk ' {print $6}'`
      sumTH=`echo "$sumTH + $tmpTH" | bc`
      echo "$i in $each_exp_num : tmpTH: $tmpTH"

      # initialize min/max throughput
      if [ $i -eq 1 ]; then
          minTH=$tmpTH
          maxTH=$tmpTH
      else
          # update max throughput
          if [ $tmpTH -gt $maxTH ]; then
              maxTH=$tmpTH
          fi
          # update min throughput
          if [ $tmpTH -lt $minTH ]; then
              minTH=$tmpTH
          fi
      fi

    done
    avgTH=`echo "$sumTH / $each_exp_num" | bc`
    echo "$thread $minTH $avgTH $maxTH" >> $result
  done
done
