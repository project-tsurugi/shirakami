set -e

build() {
    echo "start build"
    cd ../../
    rm -rf build_wp_release
    mkdir build_wp_release
    cd build_wp_release
    # $1 is 1 for pure, 2 for r-anti-track
    if test $1 -eq 1; then
        cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DBUILD_WP=ON ..
    elif test $1 -eq 2; then
        cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DBUILD_WP=ON -DBCC_7=ON ..
    else
        echo "BAG $LINENO"
        exit
    fi
    cmake --build . --target all -- -j
    #return original position.
    cd ../bench/bcc_7
}

exp() {
    echo "start exp"
    cd ../../build_wp_release/bench/bcc_7/

    duration=5
    epoch=5
    result="bcc_7.dat"
    rm -f $result

    total_trial_num=0
    for ((rec = 1000; rec <= 10000000; rec *= 10)); do
        for ((i = 1; i <= epoch; ++i)); do
            total_trial_num=$(echo "$total_trial_num + 1" | bc)
        done
    done
    echo "total_trial_num is $total_trial_num"

    trial_num=0
    for ((rec = 1000; rec <= 10000000; rec *= 10)); do
        echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_7 -d $duration -rec $rec"

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
            sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ./bcc_7 -d $duration -rec $rec >exp.txt
            tmp_th=$(grep "throughput" ./exp.txt | awk '{print $2}')
            sum_th=$(echo "scale=4; $sum_th + $tmp_th" | bc | xargs printf %10.4f)
            echo "tmp_th: $tmp_th"

            if test $i -eq 1; then
                max_th=$tmp_th
                min_th=$tmp_th
            fi

            flag=$(echo "$tmp_th > $max_th" | bc)
            if test $flag -eq 1; then
                max_th=$tmp_th
            fi

            flag=$(echo "$tmp_th < $min_th" | bc)
            if test $flag -eq 1; then
                min_th=$tmp_th
            fi

        done
        avg_th=$(echo "scale=4; $sum_th / $epoch" | bc | xargs printf %10.4f)
        echo "$rec $avg_th $min_th $max_th" >>$result
    done

    #return original pos
    cd ../../../bench/bcc_7
}

gen_graph() {
    # $1 is 1 for pure, 2 for r-anti-track
    echo "start gen_graph"
    cd ../../build_wp_release/bench/bcc_7
    if test $1 -eq 1; then
        gnuplot -e 'tname="pure"' ./../../../bench/bcc_7/script/graph.plt
    elif test $1 -eq 2; then
        gnuplot -e 'tname="ra"' ./../../../bench/bcc_7/script/graph.plt
    else
        echo "BAG $LINENO"
        exit
    fi

    #escap pdf
    cp bcc_7*pdf /tmp/

    #return original pos
    cd ../../../bench/bcc_7
}

main() {
    echo "start main"
    # cleanup escaped pdf
    rm -f /tmp/bcc_7*pdf
    build 1
    exp
    gen_graph 1
    build 2
    exp
    gen_graph 2
    ## return escaped pdf
    cp /tmp/bcc_7*pdf ./../../build_wp_release/bench/bcc_7/
}

main
