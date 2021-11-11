set -e

# Run this script from the project source directory (shirakami).

cpumhz=2100
#duration=100
duration=5
key_len=64
#val_len_ary=(8)
val_len_ary=(8 64)
rec=10000
#rec=10

rratio_ary=(99 80 50)
skew=0

ol_ops=10
ol_wp_rratio_ary=(0 10 50)

# Reinitialize later.
ol_thread=1

bt_ops=1000

epoch=5

# Reinitialize later.
result=hoge.dat

gen_graph_cc() {
	echo "start gen_graph_cc"
	gnuplot -e 'tname="cc only"' ./../../../bench/bcc_2b/graph.plt
}

gen_graph_wp() {
	echo "start gen_graph_wp"
	gnuplot -e 'tname="wp"' ./../../../bench/bcc_2b/graph.plt
}

decide_result_name() {
	# $1 rratio, $2 val, $3 wp_rratio
	local one="_r$1"
	local two="_v$2"
	local three="_wpr$3"
	result="data$one$two$three.dat"
}

build_cc() {
	echo "start build_cc"
	rm -rf build_cc_release
	mkdir build_cc_release
	cd build_cc_release
	cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF ..
	cmake --build . --target all -- -j
	cd ../
}

build_wp() {
	echo "start build_wp"
	rm -rf build_wp_release
	mkdir build_wp_release
	cd build_wp_release
	cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_CPR=OFF -DBUILD_WP=ON ..
	cmake --build . --target all -- -j
	cd ../
}

output_data_comment() {
	echo "start output_data_comment"
	# $1 rratio, $2 val, $3 wp_rratio
	echo "#ol-thread, avg-tps, min-tps, max-tps, avg-ops, avg-ar, min-ar, max-ar, batch avg-tps, min-tps, max-tps, avg-ar, min-ar, max-ar, avg-camiss, min-camiss, max-camiss, avg-maxrss, min-maxrss, max-maxrss" >>$result
	echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl -l ./bcc_2b -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -rec $rec -ol_ops $ol_ops -ol_rratio $rratio -ol_wp_rratio $ol_wp_rratio -ol_skew $skew -ol_thread sweep -bt_ops $rec -bt_rratio $rratio -bt_skew $skew" >>$result

}

exp() {
	echo "start exp"
	for ol_wp_rratio in "${ol_wp_rratio_ary[@]}"; do
		echo "start loop ol_wp_rratio"
		for val_len in "${val_len_ary[@]}"; do
			echo "start loop val_len"
			for rratio in "${rratio_ary[@]}"; do
				echo "start loop rratio"
				decide_result_name $rratio $val_len $ol_wp_rratio
				rm -f $result

				output_data_comment $rratio $val_len $ol_wp_rratio
				for ((thread = 28; thread <= 112; thread += 28)); do
					ol_thread=$(echo "$thread - 1" | bc)
					echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl -l ./bcc_2b -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -rec $rec -ol_ops $ol_ops -ol_rratio $rratio -ol_wp_rratio $ol_wp_rratio -ol_skew $skew -ol_thread $ol_thread -bt_ops $rec -bt_rratio $rratio -bt_skew $skew"
					echo "online thread $ol_thread, batch thread 1"

					sumOLTH=0
					sumOLAR=0
					sumBTTH=0
					sumBTAR=0
					sumCA=0
					sumMAXRSS=0
					maxOLTH=0
					maxOLAR=0
					maxBTTH=0
					maxBTAR=0
					maxCA=0
					maxMAXRSS=0
					minOLTH=0
					minOLAR=0
					minBTTH=0
					minBTAR=0
					minCA=0
					minMAXRSS=0

					for ((i = 1; i <= epoch; ++i)); do
						date
						sudo perf stat -e cache-misses,cache-references -o ana.txt numactl -l ./bcc_2b -cpumhz $cpumhz -duration $duration -key_len $key_len -val_len $val_len -rec $rec -ol_ops $ol_ops -ol_rratio $rratio -ol_wp_rratio $ol_wp_rratio -ol_skew $skew -ol_thread $ol_thread -bt_ops $rec -bt_rratio $rratio -bt_skew $skew >exp.txt
						tmpOLTH=$(grep "ol_throughput\[tps" ./exp.txt | grep tps | awk '{print $2}')
						tmpBTTH=$(grep bt_throughput ./exp.txt | grep tps | awk '{print $2}')
						tmpOLAR=$(grep ol_abort_rate ./exp.txt | awk '{print $2}')
						tmpBTAR=$(grep bt_abort_rate ./exp.txt | awk '{print $2}')
						tmpCA=$(grep cache-misses ./ana.txt | awk '{print $4}')
						tmpMAXRSS=$(grep maxrss ./exp.txt | awk '{print $2}')
						sumOLTH=$(echo "scale=4; $sumOLTH + $tmpOLTH" | bc | xargs printf %10.4f)
						sumBTTH=$(echo "scale=4; $sumBTTH + $tmpBTTH" | bc | xargs printf %10.4f)
						sumOLAR=$(echo "scale=4; $sumOLAR + $tmpOLAR" | bc | xargs printf %.4f)
						sumBTAR=$(echo "scale=4; $sumBTAR + $tmpBTAR" | bc | xargs printf %.4f)
						sumCA=$(echo "$sumCA + $tmpCA" | bc)
						sumMAXRSS=$(echo "$sumMAXRSS + $tmpMAXRSS" | bc)
						echo "tmpOLTH: $tmpOLTH, tmpOLAR: $tmpOLAR, tmpBTTH: $tmpBTTH, tmpBTAR: $tmpBTAR, tmpCA: $tmpCA"

						if test $i -eq 1; then
							maxOLTH=$tmpOLTH
							maxBTTH=$tmpBTTH
							maxOLAR=$tmpOLAR
							maxBTAR=$tmpBTAR
							maxCA=$tmpCA
							maxMAXRSS=$tmpMAXRSS
							minOLTH=$tmpOLTH
							minBTTH=$tmpBTTH
							minOLAR=$tmpOLAR
							minBTAR=$tmpBTAR
							minCA=$tmpCA
							minMAXRSS=$tmpMAXRSS
						fi

						flag=$(echo "$tmpOLTH > $maxOLTH" | bc)
						if test $flag -eq 1; then
							maxOLTH=$tmpOLTH
						fi
						flag=$(echo "$tmpBTTH > $maxBTTH" | bc)
						if test $flag -eq 1; then
							maxBTTH=$tmpBTTH
						fi
						flag=$(echo "$tmpOLAR > $maxOLAR" | bc)
						if test $flag -eq 1; then
							maxOLAR=$tmpOLAR
						fi
						flag=$(echo "$tmpBTAR > $maxBTAR" | bc)
						if test $flag -eq 1; then
							maxBTAR=$tmpBTAR
						fi
						flag=$(echo "$tmpCA > $maxCA" | bc)
						if test $flag -eq 1; then
							maxCA=$tmpCA
						fi
						flag=$(echo "$tmpMAXRSS > $maxMAXRSS" | bc)
						if test $flag -eq 1; then
							maxMAXRSS=$tmpMAXRSS
						fi

						flag=$(echo "$tmpOLTH < $minOLTH" | bc)
						if test $flag -eq 1; then
							minOLTH=$tmpOLTH
						fi
						flag=$(echo "$tmpBTTH < $minBTTH" | bc)
						if test $flag -eq 1; then
							minBTTH=$tmpBTTH
						fi
						flag=$(echo "$tmpOLAR < $minOLAR" | bc)
						if test $flag -eq 1; then
							minOLAR=$tmpOLAR
						fi
						flag=$(echo "$tmpBTAR < $minBTAR" | bc)
						if test $flag -eq 1; then
							minBTAR=$tmpBTAR
						fi
						flag=$(echo "$tmpCA < $minCA" | bc)
						if test $flag -eq 1; then
							minCA=$tmpCA
						fi
						flag=$(echo "$tmpMAXRSS < $minMAXRSS" | bc)
						if test $flag -eq 1; then
							minMAXRSS=$tmpMAXRSS
						fi

					done
					avgOLTH=$(echo "scale=4; $sumOLTH / $epoch" | bc | xargs printf %10.4f)
					avgBTTH=$(echo "scale=4; $sumBTTH / $epoch" | bc | xargs printf %10.4f)
					avgOLAR=$(echo "scale=4; $sumOLAR / $epoch" | bc | xargs printf %.4f)
					avgBTAR=$(echo "scale=4; $sumBTAR / $epoch" | bc | xargs printf %.4f)
					avgCA=$(echo "$sumCA / $epoch" | bc)
					avgMAXRSS=$(echo "$sumMAXRSS / $epoch" | bc)
					echo "$thread $avgOLTH $minOLTH $maxOLTH $avgOLAR $minOLAR $maxOLAR $bt_thread $avgBTTH $minBTTH $maxBTTH $avgBTAR $minBTAR $maxBTAR $avgCA $minCA $maxCA $avgMAXRSS $minMAXRSS $maxMAXRSS" >>$result
				done
			done
		done
	done
}

cc_exp() {
	echo "start cc_exp"
	cd build_cc_release/bench/bcc_2b
	exp
	gen_graph_cc
	cd ../../../
}

wp_exp() {
	echo "start wp_exp"
	cd build_wp_release/bench/bcc_2b
	exp
	gen_graph_wp
	cd ../../../
}

main() {
	echo "start main"
	build_cc
	cc_exp
	build_wp
	wp_exp
}

main
