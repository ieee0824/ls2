# LS2

ローカル開発・テスト用の Amazon S3 シミュレーターです。
[lqs](https://github.com/ieee0824/lqs) / [les](https://github.com/ieee0824/les) と同じく、Rust ライブラリ・SQLite 永続化・AWS 互換 HTTP アダプターで構成します。バケット・オブジェクト本体・マルチパートアップロードはすべて1つの SQLite ファイルに保存します。

> 開発・テスト用途です。認証・SigV4 署名検証・TLS・暗号化はありません。

## 起動

Rust（edition 2024 対応）が必要です。

```sh
cargo run --locked
```

| 環境変数 | 既定値 | 用途 |
| --- | --- | --- |
| `LS2_BIND_ADDR` | `127.0.0.1:9326` | HTTP 待ち受けアドレス |
| `LS2_DATABASE_PATH` | `ls2.sqlite` | SQLite ファイル |
| `LS2_ALLOW_UNAUTHENTICATED_REMOTE` | `false` | `true` で loopback 以外への bind を許可（認証なしで公開されます） |

既定では loopback でのみ起動します。停止・再起動後もデータは残ります。

```sh
curl --fail http://127.0.0.1:9326/_ls2/health
curl --fail http://127.0.0.1:9326/_ls2/status   # バケットごとのオブジェクト数・サイズ・未完了アップロード数
```

## AWS CLI / SDK

エンドポイントを `http://127.0.0.1:9326` に変え、リージョンとダミーの認証情報を設定します。**path-style アドレッシング**（`http://host/bucket/key`）のみ対応です。

```sh
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9326 s3 mb s3://my-bucket
aws --endpoint-url http://127.0.0.1:9326 s3 cp ./big.bin s3://my-bucket/big.bin   # 8 MiB 超はマルチパート
aws --endpoint-url http://127.0.0.1:9326 s3 ls --recursive s3://my-bucket
```

| SDK | 設定 |
| --- | --- |
| AWS CLI / boto3 | `endpoint_url` を指定（IP のエンドポイントは自動で path-style） |
| AWS SDK for Go v2 | `o.BaseEndpoint = aws.String("http://127.0.0.1:9326")`、`o.UsePathStyle = true` |
| AWS SDK for JavaScript v3 | `endpoint: "http://127.0.0.1:9326"`、`forcePathStyle: true` |
| AWS SDK for Java v2 | `endpointOverride(URI.create(...))`、`forcePathStyle(true)` |

署名付き URL（presigned URL）は署名を検証せずに受け付けます。

## 対応範囲

| 分類 | API |
| --- | --- |
| バケット | `ListBuckets` / `CreateBucket` / `DeleteBucket` / `HeadBucket` / `GetBucketLocation` / `GetBucketVersioning`（常に未設定） |
| オブジェクト | `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` / `DeleteObjects` / `CopyObject` |
| 一覧 | `ListObjectsV2` / `ListObjects`（prefix・delimiter・ページング・`encoding-type=url`） |
| マルチパート | `CreateMultipartUpload` / `UploadPart` / `UploadPartCopy` / `CompleteMultipartUpload` / `AbortMultipartUpload` / `ListParts` / `ListMultipartUploads` |
| タグ | `GetObjectTagging`（常に空。タグの設定は未対応） |
| LS2 独自 | `GET /_ls2/health`、`GET /_ls2/status` |

- **Range**: `bytes=a-b` / `bytes=a-` / `bytes=-n`。範囲外は 416 `InvalidRange`。複数範囲は 501。
- **条件付きリクエスト**: GET/HEAD の `If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since`、書き込みの `If-None-Match: *` / `If-Match`、コピー元の `x-amz-copy-source-if-*`。
- **整合性**: `Content-MD5`、`x-amz-content-sha256`（16進値の場合）、フレキシブルチェックサム（CRC32 / CRC32C / CRC64NVME / SHA1 / SHA256）をヘッダー・`aws-chunked` トレーラーの両方で検証し、不一致は保存しません。チェックサム未指定の PutObject には S3 と同様に CRC64NVME を付与し、`x-amz-checksum-mode: ENABLED` の GET/HEAD で返します。
- **メタデータ**: `Content-Type` / `Content-Encoding` / `Content-Disposition` / `Content-Language` / `Cache-Control` / `Expires` / `x-amz-meta-*`（合計 2 KiB まで）。GET の `response-*` クエリによる上書き。
- **エラー**: S3 形式の XML（`Code` / `Message` / `RequestId`）と `x-amz-request-id` を返します。HEAD と 304 は本文なしです。

## 制約

**未対応の機能は黙って無視せず `501 NotImplemented` を返します。** 例: バージョニングの有効化、ACL・ポリシー、タグ設定、SSE、Object Lock、`STANDARD` 以外のストレージクラス、ライフサイクル、CORS、通知、virtual-hosted style、`partNumber` 指定の GET。許可リストにない `x-amz-*` ヘッダーやクエリパラメーターも 501 になるため、ローカルでは成功したのに本番では意味が違う、というケースを減らせます。

- 1リクエストの本文は 256 MiB まで（超過は `EntityTooLarge`）。それより大きいオブジェクトはマルチパートで作成します。GET はレスポンス範囲をメモリに読み込みます。
- マルチパートは S3 と同じく最後以外のパートが 5 MiB 以上、パート番号 1〜10000 です。完成したオブジェクトにはオブジェクト単位のチェックサムを付けません（パート単位では検証します）。
- CopyObject はコピー元の ETag とチェックサムを引き継ぎます（S3 ではマルチパート由来の ETag が変わる場合があります）。
- us-east-1 のバケットを再作成すると成功し、ほかのリージョンでは `BucketAlreadyOwnedByYou` になります（S3 の挙動に合わせています）。`DeleteBucket` は未完了のマルチパートアップロードを一緒に破棄します。
- アカウントは1つだけです。`x-amz-expected-bucket-owner` は `000000000000` 以外を `AccessDenied` にします。
- SQLite は平文保存で、自動削除はしません。HTTP からの操作は1接続で直列に処理します。

## Rust ライブラリ

HTTP サーバーを介さずに使えます。時刻は呼び出し側が Unix ミリ秒で渡します。

```rust
use ls2::{Ls2, PutOptions, ReadCondition};

let mut s3 = Ls2::in_memory()?; // 永続化: Ls2::open("ls2.sqlite")?
s3.create_bucket("my-bucket", None, 1_000)?;
let info = s3.put_object("my-bucket", "hello.txt", b"hello", PutOptions::default(), 1_000)?;
let out = s3.get_object("my-bucket", "hello.txt", None, &ReadCondition::default())?;
assert_eq!((out.data.as_slice(), out.info.etag), (&b"hello"[..], info.etag));
# Ok::<(), ls2::Error>(())
```

## 開発

```sh
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
# 別ターミナルで LS2 を起動した状態で実行（AWS CLI が必要）
bash integration/aws-cli.sh
```

設計・拡張方針は [DESIGN.md](DESIGN.md) を参照してください。
