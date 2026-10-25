# LS2 設計

## lqs / les から採用する方針

[lqs の設計](https://github.com/ieee0824/lqs/blob/main/DESIGN.md)・[les の設計](https://github.com/ieee0824/les/blob/main/DESIGN.md)と同じく、同期コアを HTTP から独立させ、SQLite を状態の唯一の保存先にします。オブジェクト本体も SQLite に保存し、別ディレクトリのファイルとの不整合が起きない構成にします。インメモリ DB とファイル DB は同じスキーマを使います。コアへの時刻は呼び出し元がミリ秒で渡し、HTTP 層だけがホストの時計を読みます。

| ファイル | 役割 |
| --- | --- |
| `src/lib.rs` | バケット・オブジェクト・一覧・コピー・条件付きリクエストの同期コア |
| `src/multipart.rs` | マルチパートアップロード（コアの続き） |
| `src/checksum.rs` | MD5 とフレキシブルチェックサムの計算・検証 |
| `src/server.rs` | S3 REST/XML の変換、`aws-chunked` の復号、許可リスト |
| `src/main.rs` | 環境変数・DB 接続・bind 制限・サーバー起動 |

HTTP からも Rust からも同じ検証を通ります。HTTP は `Mutex` で1接続を直列に使い、WAL と5秒の busy timeout を設定します。

## 保存

```text
buckets(name, region, created_at_ms)
objects(id, bucket, key BLOB, size, etag, last_modified_ms, metadata JSON, checksum JSON)  UNIQUE(bucket, key)
chunks(object_id, seq, offset, size, data BLOB)                                            PK(object_id, seq)
uploads(id, bucket, key BLOB, initiated_at_ms, metadata JSON, checksum_algorithm)
parts(upload_id, part_number, size, etag, last_modified_ms, checksum JSON, data BLOB)      PK(upload_id, part_number)
```

- **キーは BLOB** で保存します。SQLite の BLOB 比較は memcmp なので、S3 と同じ UTF-8 バイト順で一覧できます。prefix の範囲は「prefix 以上、末尾バイトを1つ増やした値未満」で索引を使います。
- **本体はチャンク**に分けます。単一 PUT は1チャンク、マルチパートはパートごとに1チャンクです。Complete は `INSERT ... SELECT` でパートをチャンクへ移し、Rust 側で全データを連結しません。Range 読み出しは対象チャンクだけを `substr` で切り出します。SQLite の BLOB 上限（約1 GB）は1チャンクにだけ適用されます。
- 上書き・削除は外部キーの `ON DELETE CASCADE` でチャンクも消します。上書きは「旧行削除 → 新行挿入 → チャンク挿入」を1つの Immediate トランザクションで行い、途中失敗では旧オブジェクトが残ります。
- 整合性チェック（MD5・チェックサム）はトランザクションの前に行い、不一致なら何も保存しません。
- `PRAGMA user_version` にスキーマ版を記録し、新しい版の DB は開きません。スキーマ変更時は明示的なマイグレーションを追加します。

## 一覧

`list_objects` は `(位置, 含むか)` のカーソルで 256 行ずつ読み、delimiter を見つけたら common prefix を1件として数え、その prefix の上限までカーソルを飛ばします。common prefix 配下の大量のキーを1件ずつ読みません。キーと common prefix は合わせて `max-keys` に数え、最後に返した値を次の開始位置（v2 はその Base64 を continuation token、v1 は NextMarker）にします。開始位置以下の common prefix は返しません。

## HTTP 層

すべての S3 リクエストを1つの fallback ハンドラーで受け、生のパスを自前で `/{bucket}/{key}` に分けて percent-decode します。ルーターのパス正規化に任せないため、`//` を含むキーや末尾 `/` のキーもそのまま扱えます。独自 API は S3 のバケット名に使えない `_` で始まる `/_ls2/` に置き、`/health` のようなバケット名と衝突しません。

本文は 256 MiB まで読み、次の順で処理します。

1. `x-amz-content-sha256` が `STREAMING-*` または `Content-Encoding: aws-chunked` なら、チャンクを復号してトレーラーを取り出します（チャンク署名は検証しません）。保存する `Content-Encoding` からは `aws-chunked` を除きます。
2. `x-amz-decoded-content-length` と `x-amz-trailer` の宣言どおりかを確認します。
3. 16進の `x-amz-content-sha256`、`Content-MD5`、`x-amz-checksum-*`（ヘッダーまたはトレーラー）を検証します。本文を保存しない操作（DeleteObjects・Complete など）もここで検証します。

署名・SDK 用のクエリ（`X-Amz-Signature`、`x-id` など）は取り除いてから操作を判定します。

### 黙って無視しない

操作ごとにクエリパラメーターと `x-amz-*` ヘッダーの許可リストを持ち、外れたものは `501 NotImplemented` にします。たとえば `?acl` や `x-amz-tagging` を無視して成功させると、ローカルでは通るのに本番では意味が違うコードを見逃します。les と同じく、未対応は明示的なエラーにする方針です。AWS CLI のコピーが送る `x-amz-copy-source-if-match` は、この仕組みで見つけて対応しました。

S3 自身が無視する入力（COPY 指定時のメタデータヘッダーなど）は S3 に合わせて無視します。

## 互換性の境界

- ETag は単一 PUT が MD5、マルチパートが「各パート MD5 を連結した MD5」-パート数です。
- チェックサム未指定の PutObject には、S3 の既定動作と同じく CRC64NVME を付けます。マルチパートの完成オブジェクトにはオブジェクト単位のチェックサムを付けず、パート単位で検証・保存します。SDK はチェックサムが返らないオブジェクトの検証を省略します。
- 条件付き GET は S3 の優先順位（If-Match が If-Unmodified-Since に、If-None-Match が If-Modified-Since に優先）に従い、時刻は秒単位で比べます。コピー元条件の失敗はすべて 412 です。
- アップロード ID はバケットとキーに紐づき、別のキーからは `NoSuchUpload` になります。
- エラーは S3 形式の XML です。HEAD と 304 は本文を返せないため、デバッグ用に `x-amz-error-code` ヘッダーを付けます。

## テスト

`tests/simulator.rs` はコア（バケット、Range、整合性エラー時に保存されないこと、条件付きリクエスト、common prefix をまたぐページング、コピー、マルチパートの各エラー、DB 再オープン）と HTTP（aws-chunked + トレーラー、エラー XML、501、一覧 XML、DeleteObjects、マルチパート）を確認します。時刻を注入するので sleep はありません。`integration/aws-cli.sh` は実際の AWS CLI で、20 MiB のマルチパートアップロード・分割ダウンロード・UploadPartCopy・sync を確認します。

## 今後の拡張

- **P2**: ブラウザでバケットとオブジェクトを閲覧する画面（les のメール閲覧と同様、サーバー側で HTML を生成）、virtual-hosted style（`<bucket>.localhost`）、Docker イメージ。
- **P3**: バージョニング（`objects` に version 列を追加するマイグレーション）、オブジェクト/バケットのタグ、CORS（ブラウザから直接アップロードする開発用）、ライフサイクルによる期限切れ削除（注入した時刻で決定的にテスト）、lqs へのイベント通知（S3 → SQS）。
- 大きなオブジェクトの GET をチャンク単位のストリーミングにすること、マルチパートの完成オブジェクトに FULL_OBJECT / COMPOSITE チェックサムを付けることも検討対象です。
