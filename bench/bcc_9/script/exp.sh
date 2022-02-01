set -e

build() {
    echo "start build"
    cd ../../
    rm -rf build_cc_release
    mkdir build_cc_release
    cd build_cc_release
    # $1 is 1 for vec, 2 for um
    if test $1 -eq 1; then
        cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DPARAM_VAL_PRO=0 ..
    elif test $1 -eq 2; then
        cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DPARAM_VAL_PRO=1 ..
    else
        echo "BAG $LINENO"
        exit
    fi
    cmake --build . --target all -- -j
    #return original position.
    cd ../bench/bcc_9
}

exp() {
    echo "start exp"
    cd ../../build_cc_release/bench/bcc_9/

    duration=5
    epoch=5
    skew=0
    tx_size=10
    th_sizes=(28 56 84 112)
    result="bcc_9.dat"
    rm -f $result

    total_trial_num=0
    for th_size in "${th_sizes[@]}"; do
        for ((i = 1; i <= epoch; ++i)); do
            total_trial_num=$(echo "$total_trial_num + 1" | bc)
        done
    done

    echo "total_trial_num is $total_trial_num"

    trial_num=0
    for th_size in "${th_sizes[@]}"; do
        echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_9 -d $duration -th $th_size"

        sum_th=0
        sum_mm=0
        max_th=0
        max_mm=0
        min_th=0
        min_mm=0

        for ((i = 1; i <= epoch; ++i)); do
            trial_num=$(echo "$trial_num + 1" | bc)
            rate_to_all=$(echo "scale=4; $trial_num / $total_trial_num" | bc | xargs printf %.4f)
            echo "rate_to_all is $rate_to_all"
            perc_rate_to_all=$(echo "$rate_to_all * 100" | bc | xargs printf %2.4f)
            echo "perc_rate_to_all is $perc_rate_to_all"

            date
            sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_9 -d $duration -th $th_size >exp.txt
            tmp_th=$(grep "throughput" ./exp.txt | awk '{print $2}')
            tmp_mm=$(grep "maxrss" ./exp.txt | awk '{print $2}')
            sum_th=$(echo "scale=4; $sum_th + $tmp_th" | bc | xargs printf %10.4f)
            sum_mm=$(echo "scale=4; $sum_mm + $tmp_mm" | bc | xargs printf %10.4f)
            echo "tmp_th: $tmp_th, tmp_mm: $tmp_mm"

            if test $i -eq 1; then
                max_th=$tmp_th
                min_th=$tmp_th
                max_mm=$tmp_mm
                min_mm=$tmp_mm
            fi

            flag=$(echo "$tmp_th > $max_th" | bc)
            if test $flag -eq 1; then
                max_th=$tmp_th
            fi

            flag=$(echo "$tmp_th < $min_th" | bc)
            if test $flag -eq 1; then
                min_th=$tmp_th
            fi

            flag=$(echo "$tmp_mm > $max_mm" | bc)
            if test $flag -eq 1; then
                max_mm=$tmp_mm
            fi

            flag=$(echo "$tmp_mm < $min_mm" | bc)
            if test $flag -eq 1; then
                min_mm=$tmp_mm
            fi
        done
        avg_th=$(echo "scale=4; $sum_th / $epoch" | bc | xargs printf %10.4f)
        avg_mm=$(echo "scale=4; $sum_mm / $epoch" | bc | xargs printf %10.4f)
        echo "$th_size $avg_th $min_th $max_th $avg_mm $min_mm $max_mm" >>$result
    done

    #return original pos
    cd ../../../bench/bcc_9
}

gen_graph() {
    # $1 is 1 for vector, 2 for unordered_map
    echo "start gen_graph"
    cd ../../build_cc_release/bench/bcc_9
    if test $1 -eq 1; then
        gnuplot -e 'tname="rcu"' ./../../../bench/bcc_9/script/graph.plt
    elif test $1 -eq 2; then
        gnuplot -e 'tname="rw"' ./../../../bench/bcc_9/script/graph.plt
    else
        echo "BAG $LINENO"
        exit
    fi

    #escap pdf
    cp bcc_9*pdf /tmp/

    #return original pos
    cd ../../../bench/bcc_9
}

main() {
    echo "start main"
    # cleanup escaped pdf
    rm -f /tmp/bcc_9*pdf
    #build 1
    #exp
    #gen_graph 1
    build 2
    exp
    gen_graph 2
    ### return escaped pdf
    cp /tmp/bcc_9*pdf ./../../build_cc_release/bench/bcc_9/
}

main
