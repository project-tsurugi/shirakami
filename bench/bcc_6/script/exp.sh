set -e

build() {
    echo "start build"
    cd ../../
    rm -rf build_wp_release
    mkdir build_wp_release
    cd build_wp_release
    cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF ..
    cmake --build . --target all -- -j
    #return original position.
    cd ../bench/bcc_6
}

exp() {
    echo "start exp"
    cd ../../build_wp_release/bench/bcc_6/

    duration=5
    epoch=5
    result="bcc_6.dat"
    rm -f $result

    total_trial_num=0
    for ((th = 28; th <= 112; th += 28)); do
        for ((i = 1; i <= epoch; ++i)); do
            total_trial_num=$(echo "$total_trial_num + 1" | bc)
        done
    done
    echo "total_trial_num is $total_trial_num"

    trial_num=0
    for ((th = 28; th <= 112; th += 28)); do
        echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_6 -th $th -d $duration "

        sum_ver=0
        max_ver=0
        min_ver=0
        sum_th=0
        max_th=0
        min_th=0

        for ((i = 1; i <= epoch; ++i)); do
            trial_num=$(echo "$trial_num + 1" | bc)
            rate_to_all=$(echo "scale=4; $trial_num / $total_trial_num" | bc | xargs printf %.4f)
            echo "rate_to_all is $rate_to_all"
            perc_rate_to_all=$(echo "$rate_to_all * 100" | bc | xargs printf %2.4f)
            echo "perc_rate_to_all is $perc_rate_to_all"

            date
            sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_6 -th $th -d $duration >exp.txt
            tmp_ver=$(grep "gc_ver" ./exp.txt | awk '{print $2}')
            tmp_th=$(grep "throughput" ./exp.txt | awk '{print $2}')
            sum_ver=$(echo "scale=4; $sum_ver + $tmp_ver" | bc | xargs printf %10.4f)
            sum_th=$(echo "scale=4; $sum_th + $tmp_th" | bc | xargs printf %10.4f)
            echo "tmp_ver: $tmp_ver, tmp_th: $tmp_th"

            if test $i -eq 1; then
                max_ver=$tmp_ver
                min_ver=$tmp_ver
                max_th=$tmp_th
                min_th=$tmp_th
            fi

            flag=$(echo "$tmp_ver > $max_ver" | bc)
            if test $flag -eq 1; then
                max_ver=$tmp_ver
            fi
            flag=$(echo "$tmp_th > $max_th" | bc)
            if test $flag -eq 1; then
                max_th=$tmp_th
            fi

            flag=$(echo "$tmp_ver < $min_ver" | bc)
            if test $flag -eq 1; then
                min_ver=$tmp_ver
            fi
            flag=$(echo "$tmp_th < $min_th" | bc)
            if test $flag -eq 1; then
                min_th=$tmp_th
            fi

        done
        avg_ver=$(echo "scale=4; $sum_ver / $epoch" | bc | xargs printf %10.4f)
        avg_th=$(echo "scale=4; $sum_th / $epoch" | bc | xargs printf %10.4f)
        echo "$th $avg_ver $min_ver $max_ver $avg_th $min_th $max_th" >>$result
    done
    #return original pos
    cd ../../../bench/bcc_6
}

gen_graph() {
    echo "start gen_graph"
    cd ../../build_wp_release/bench/bcc_6
    gnuplot ./../../../bench/bcc_6/script/graph.plt
    #return original pos
    cd ../../../bench/bcc_6
}

main() {
    echo "start main"
    build
    exp
    gen_graph
}

main
