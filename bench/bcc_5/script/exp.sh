set -e

build() {
    echo "start build"
    cd ../../
    rm -rf build_wp_release
    mkdir build_wp_release
    cd build_wp_release
    cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DBUILD_WP=ON ..
    cmake --build . --target all -- -j
    #return original position.
    cd ../bench/bcc_5
}

exp() {
    echo "start exp"
    cd ../../build_wp_release/bench/bcc_5/

    duration=5
    epoch=5
    result="bcc_5.dat"
    rm -f $result

    total_trial_num=0
    for ((cr = 0; cr <= 100; cr += 25)); do
        for ((i = 1; i <= epoch; ++i)); do
            total_trial_num=$(echo "$total_trial_num + 1" | bc)
        done
    done
    echo "total_trial_num is $total_trial_num"

    trial_num=0
    for ((cr = 0; cr <= 100; cr += 25)); do
        echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl -l ./bcc_5 -cr $cr -d $duration "

        sumTH=0
        maxTH=0
        minTH=0

        for ((i = 1; i <= epoch; ++i)); do
            trial_num=$(echo "$trial_num + 1" | bc)
            rate_to_all=$(echo "scale=4; $trial_num / $total_trial_num" | bc | xargs printf %.4f)
            echo "rate_to_all is $rate_to_all"
            perc_rate_to_all=$(echo "$rate_to_all * 100" | bc | xargs printf %2.4f)
            echo "perc_rate_to_all is $perc_rate_to_all"

            date
            sudo perf stat -e cache-misses,cache-references -o ana.txt numactl -l ./bcc_5 -cr $cr -d $duration >exp.txt
            tmpTH=$(grep "ol_throughput\[tps" ./exp.txt | awk '{print $2}')
            sumTH=$(echo "scale=4; $sumTH + $tmpTH" | bc | xargs printf %10.4f)
            echo "tmpTH: $tmpTH"

            if test $i -eq 1; then
                maxTH=$tmpTH
                minTH=$tmpTH
            fi
            flag=$(echo "$tmpTH > $maxTH" | bc)
            if test $flag -eq 1; then
                maxTH=$tmpTH
            fi
            flag=$(echo "$tmpTH < $minTH" | bc)
            if test $flag -eq 1; then
                minTH=$tmpTH
            fi
        done
        avgTH=$(echo "scale=4; $sumTH / $epoch" | bc | xargs printf %10.4f)
        echo "$cr $avgTH $minTH $maxTH" >>$result
    done
    #return original pos
    cd ../../../bench/bcc_5
}

gen_graph() {
    echo "start gen_graph"
    cd ../../build_wp_release/bench/bcc_5
    gnuplot ./../../../bench/bcc_5/script/graph.plt
    #return original pos
    cd ../../../bench/bcc_5
}

main() {
    echo "start main"
    build
    exp
    gen_graph
}

main
