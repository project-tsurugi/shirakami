# エポックをまたぐ OCC (short_tx) コミットの検出

OCC は原則として最新 epoch のバージョンへ書き込みを行なうため, current epoch より前の epoch の snapshot は OCC により書き換えられることはない.
例外としては, commit 処理と並行して global epoch が増加した場合であり, その場合には OCC は前の epoch へ引き続き書き込みを続けるため,
結果として current epoch より前の epoch に対して OCC が書き込みを行なうことになる.
そのため, 読む側のトランザクションは, OCC がその読みたい対象を書き換える可能性があるかどうかを気にする必要がある.

その検出機構について記述する.

## session::short_expose_ongoing_status

基本動作

* 各トランザクションは short_expose_ongoing_status というメンバ変数を持ち, この値は lock bit と epoch 数値を持つ.
* epoch manager は global epoch 増加直後に (epoch 増加に伴う諸作業の一環として) 全トランザクションの short_expose_ongoing_status に対して
    * lock が取られていなければ, epoch を新しい epoch で上書きする.
    * lock が取られていればそのままにする.
    * 上記の「全トランザクション」は, 文字通り, 開始していないトランザクションや OCC 以外のトランザクションも含む全部である.
* OCC トランザクションは commit による Record 書き換え (expose) の
    * 処理前に short_expose_ongoing_status の lock を取る.
    * 処理後に short_expose_ongoing_status の lock を解放し, ついでに epoch 部分を current global epoch で更新する.
        * lock 中に epoch manager から epoch 部分が更新されなかったことの補償のためである.

以上により, 各トランザクションの short_expose_ongoing_status の epoch 部分を見ると, このトランザクションがこれより小さい epoch のバージョンに書かないことを保証できる.

## epoch::min_epoch_occ_potentially_write

全トランザクションの short_expose_ongoing_status の epoch 部分の最小値は, この値以下のバージョンの Record は OCC により書き換えられる可能性があることを示し,
重要な値であるが, この値の計算は重いため値を epoch::min_epoch_occ_potentially_write としてキャッシュしておく.

この最小値がある epoch E より小さいかを確認したい場合には, まずキャッシュである epoch::min_epoch_occ_potentially_write と E を比較し,
小さかった場合には小さいと判断, 小さくなかった場合には念のため全トランザクションのほうを見て最小値を取ったもので再確認する.
(チェックの精度が低くてもよいならば再確認は省略する)

この最小値が変更される可能性があるのは global epoch 増加直後と OCC が lock を解放したタイミングのみであるため,
この両方のタイミングで最小値を再計算すればキャッシュ値を最新に保てるが,
OCC の諸処理に負荷を掛けたくないという設計上の理由により, OCC の lock 解放時には実行していない.
