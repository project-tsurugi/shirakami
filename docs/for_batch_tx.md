# ロング（バッチ）トランザクションのための機構
執筆開始 : 2020/12/01 田辺

## long read transaction への対策
todo
- Philosophy : todo

## long read write transaction への対策
バッチ処理（ロングトランザクション）を確実にコミットさせながら，オンライン処理（ショートトランザクション）の高い性能を発揮する需要がある
(ex. パン屋さんの原価計算処理）． 本セクションでは read / write が混在するロングトランザクションを想定し，執筆する．
- Philosophy : serialization order において, long tx の過去に飛ばすか未来に飛ばす．
Other tx が long tx をまたいだらアボートする．
以下non-deterministic を想定する（deterministic であれば，より柔軟に構築する）. 

- long tx は read phase で読んだレコードに predicate lock (for preventing from phantom), ロックA をかける．
Predicate にひっかかる insert は phantom 問題を起こさないために，long tx が終わるまで許容しない．
ロック A のかかったレコードには other tx は long tx’s read が validation phase で read verify に失敗しないようにするため write 
できない(未来を待つ)．
Other tx がロック A を読んで no lock なレコードに書くのは許容する (read other write)．
その場合，serialization order としては，並置か過去となる．

- ロックA の解放待ちをしている other tx’s write が多い場合，ロックA をロック B にアップグレードし，直後のステップとなる other tx’s read 
を許容せず，blind write だけを許容することもできる. 
換言すると blind write のトランザクションを serialization order 上の未来に置く．
Read が許容できない状況は batch’s write が来るかもしれなくて，read はそれを読まないといけないからである．
Blind write を許容した場合，そのトランザクション A はバッチトランザクションよりも未来の順序となるため，直後から並行トランザクションによる読み込み操作
はトランザクション A よりも未来の直列化順序という前提でバッチトランザクションの終わりを待たなくてよくなる．
そのような挙動をさせる場合は新しいバージョンを設けて，そのバージョンに上書きしていく(2VCC)．
Silo が write set に対して max (each (tid + 1)) する理由は read への通知なため（？），そのレコードだけに関してこれを省ける
（場合によっては invisible write にできる）．
新しいバージョンが来てからは，そのバージョンに対して以降通常の Silo cc を適用できる（未来に置く）．
そうなると，batch’s write もそのレコードに対して適用できる（バッチより未来の直列化順序となるトランザクションの blind write によって 
バッチの write が invisible になる）．
