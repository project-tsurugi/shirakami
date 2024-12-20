# RTX Barrier に関するデザイン

2024-12-04 arakawa (NT)

## やりたいこと

* RTX トランザクションが読むデータに、特定のトランザクションの結果が確実に含まれるようにしたい

## 方針

* 基盤的な仕組みとして、特定エポックの safe ss の完成を待ち合わせるための仕組み (バリア) をクライアントに提供する
* 読みたい対象のトランザクション完了後にバリアを作成し、バリアが解放されるまで待ったのちにRTXを実行することで、前者のトランザクションの結果を後者のRTXで読むことができるようにする

## 実装案

### デザイン

1. バリアの作成
   1. クライアントは、サーバに対して、バリアの作成をリクエストする
   2. サーバは、その時点までにコミットされたトランザクションが含まれるスナップショット位置 (通常はエポック番号) を計算し、クライアントに返す
      * OCC を含めて考えると、エポック番号は current epoch + 1 くらいを想定
      * このスナップショット位置が、セーフスナップショットに含まれれば、以降のRTXで読めるようになる
   3. クライアントは、スナップショット位置を「バリアオブジェクト」として保持する
2. バリアの状態確認
   1. クライアントは、バリアオブジェクトに含まれるスナップショット位置を指定して、サーバに対してバリアの状態確認をリクエストする
   2. サーバは、指定されたスナップショット位置がセーフスナップショットに含まれていれば、バリアが解放されたとみなしてクライアントにその旨を返す
      * このとき、稼働中の LTX がいるとそれらが払底するまで待ち合わせる場合がある
   3. クライアントは、サーバからのレスポンスを受け取り、バリアが解放されたかどうかを判断する

### Protobuf

```protobuf
// Represents a barrier for RTX.
message RtxBarrier {
    // the epoch number of the barrier.
    uint64 anchor = 1;
}
```

```protobuf
// A request to acquire a barrier for RTX transactions.
message request.AcquireRtxBarrier {}
```

```protobuf
// A response of request.AcquireRtxBarrier.
message response.AcquireRtxBarrier {

    // acquiring RTX barrier is successfully completed.
    message Success {
        // the barrier for RTX.
        common.RtxBarrier barrier = 1;
    }

    // the response body.
    oneof result {
        // request is successfully completed.
        Success success = 1;

        // engine error occurred.
        Error error = 2;
    }
}
```

```protobuf
// A request to check state of the RTX Barrier.
message request.CheckRtxBarrier {
    RtxBarrier barrier = 1;
}
```

```protobuf
// A response of request.CheckRtxBarrier.
message response.CheckRtxBarrier {

    // acquiring RTX barrier is successfully completed.
    message Success {
        // whether the barrier has been released.
        bool released = 1;

        // TODO: more information (e.g., LTX is running, etc.)
    }

    // the response body.
    oneof result {
        // request is successfully completed.
        Success success = 1;

        // engine error occurred.
        Error error = 2;
    }
}
```

### Java

```java
interface SqlClient {
    /**
     * Creates a new barrier for RTX transactions.
     * <p>
     * This barrier ensures RTXs to read the committed transactions before the barrier is created.
     * That is, after the resulting barrier is released, the any RTXs can read the committed transactions before the barrier was acquired.
     * </p>
     * @return a future response of this request:
     *    provides a barrier for the RTX transactions
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<RtxBarrier> acquireRtxBarrier() throws IOException;
}
```

```java
/**
 * Represents Barrier that ensures RTXs to read the committed transactions before the barrier is created.
 */
interface RtxBarrier {

    /**
     * Checks if the barrier has been released.
     * @return a future response of this request:
     *    provides {@code true} if the barrier has been released, {@code false} otherwise
     * @throws IOException if I/O error was occurred while sending request
     * @throws IllegalStateException if the barrier is already disposed
     */
    FutureResponse<Boolean> isReleased() throws IOException;
}
```

* 備考
  * `RtxBarrier` は厳密にはサーバ側リソースであるが、固有のリソースを確保していないため解放の必要はない
  * `RtxBarrier` は、便利メソッドとして自動的に待ち合わせを行う `await` などを入れてもよいが、ここでは最低限とした

### tgsql

バリア付き RTX:

```sql
START TRANSACTION; -- OCC
INSERT INTO t1 VALUES (1);
COMMIT;

-- RTX WITH BARRIER (StartTransactionStatement.properties[BARRIER]=true)
START TRANSACTION
    READ ONLY
    WITH BARRIER;
```

手動バリア:

```sql
START TRANSACTION; -- OCC
INSERT INTO t1 VALUES (1);
COMMIT;

-- acquire a barrier and wait until it is released
\barrier

-- RTX WITHOUT BARRIER
START TRANSACTION
    READ ONLY
    WITH BARRIER=false;
```

* 備考
  * デフォルトのバリアの有無はクライアント設定で変更できてもよい
    * ただし、デフォルトONにすると LTX がいるときに RTX が始まらなくなる

## その他

* 将来的に、 read area を指定してバリアを作成する、などの拡張が考えられうる
