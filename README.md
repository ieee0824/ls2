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
| `LS2_VIRTUAL_HOST_DOMAINS` | `localhost` | virtual-hosted style として扱うドメイン（カンマ区切り）。`<bucket>.<domain>` の Host をバケットとみなします |

既定では loopback でのみ起動します。停止・再起動後もデータは残ります。

```sh
curl --fail http://127.0.0.1:9326/_ls2/health
curl --fail http://127.0.0.1:9326/_ls2/status   # バケットごとのオブジェクト数・サイズ・未完了アップロード数
```

### Docker

```sh
docker build -t ls2 .
docker run --rm -p 127.0.0.1:9326:9326 -v ls2-data:/data ls2
```

イメージは `LS2_BIND_ADDR=0.0.0.0:9326`・`LS2_ALLOW_UNAUTHENTICATED_REMOTE=true`・`LS2_DATABASE_PATH=/data/ls2.sqlite` で起動します。認証がないため、ポートは `127.0.0.1` に限定して公開してください。データはボリューム `/data` に残ります。

## 閲覧画面

ブラウザで `http://127.0.0.1:9326/_ls2/browse` を開くと、バケット一覧 → フォルダ（`/` 区切りの prefix）→ オブジェクト詳細の順にたどれます。詳細画面ではサイズ・ETag・チェックサム・メタデータ・UTF-8 テキストの先頭 64 KiB を表示し、ダウンロードできます。

- 読み取り専用です。表示する値はすべて HTML エスケープし、CSP でスクリプトを禁止しています。
- ダウンロードは常に `attachment`（`application/octet-stream`、`nosniff`）で返すため、保存された HTML や SVG がブラウザで実行されることはありません。

## AWS CLI / SDK

エンドポイントを `http://127.0.0.1:9326`（path-style）または `http://localhost:9326`（virtual-hosted style 可）に変え、リージョンとダミーの認証情報を設定します。

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

**virtual-hosted style**: エンドポイントを `http://localhost:9326` にすると、SDK は `http://<bucket>.localhost:9326/<key>` に送ります（Go v2 は `UsePathStyle = false`）。`*.localhost` を 127.0.0.1 に解決しない環境では、hosts への追加か、HTTP クライアント側で接続先を 127.0.0.1 に固定してください（`integration/s3_test.go` を参照）。ほかのホスト名で使う場合は `LS2_VIRTUAL_HOST_DOMAINS` に追加します。IP アドレスや `localhost` そのもの、未登録のホスト名（Docker のサービス名など）は path-style として扱います。

署名付き URL（presigned URL）は署名を検証せずに受け付けます。SDK がクエリに移した `X-Amz-*` ヘッダー（`X-Amz-Checksum-Mode` など）はヘッダーとして扱います。

## 対応範囲

| 分類 | API |
| --- | --- |
| バケット | `ListBuckets` / `CreateBucket` / `DeleteBucket` / `HeadBucket` / `GetBucketLocation` |
| バージョニング | `PutBucketVersioning` / `GetBucketVersioning` / `ListObjectVersions`、各操作の `versionId`、削除マーカー |
| オブジェクト | `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` / `DeleteObjects` / `CopyObject` |
| 一覧 | `ListObjectsV2` / `ListObjects`（prefix・delimiter・ページング・`encoding-type=url`） |
| マルチパート | `CreateMultipartUpload` / `UploadPart` / `UploadPartCopy` / `CompleteMultipartUpload` / `AbortMultipartUpload` / `ListParts` / `ListMultipartUploads` |
| タグ | `Put/Get/DeleteObjectTagging`（`versionId` 可）、`Put/Get/DeleteBucketTagging`、PutObject / CopyObject / CreateMultipartUpload の `x-amz-tagging` |
| CORS | `Put/Get/DeleteBucketCors`、preflight（`OPTIONS`）、許可されたオリジンからのリクエストへの `Access-Control-*` |
| LS2 独自 | `GET /_ls2/health`、`GET /_ls2/status`、`GET /_ls2/browse`（閲覧画面） |

- **Range**: `bytes=a-b` / `bytes=a-` / `bytes=-n`。範囲外は 416 `InvalidRange`。複数範囲は 501。
- **条件付きリクエスト**: GET/HEAD の `If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since`、書き込みの `If-None-Match: *` / `If-Match`、コピー元の `x-amz-copy-source-if-*`。
- **整合性**: `Content-MD5`、`x-amz-content-sha256`（16進値の場合）、フレキシブルチェックサム（CRC32 / CRC32C / CRC64NVME / SHA1 / SHA256）をヘッダー・`aws-chunked` トレーラーの両方で検証し、不一致は保存しません。チェックサム未指定の PutObject には S3 と同様に CRC64NVME を付与し、`x-amz-checksum-mode: ENABLED` の GET/HEAD で返します。
- **メタデータ**: `Content-Type` / `Content-Encoding` / `Content-Disposition` / `Content-Language` / `Cache-Control` / `Expires` / `x-amz-meta-*`（合計 2 KiB まで）。GET の `response-*` クエリによる上書き。
- **バージョニング**: 有効化後の書き込みごとに新しいバージョン ID を付け、`versionId` なしの削除は削除マーカーを作ります。`GET` / `HEAD` / `DELETE` / `CopyObject`（`x-amz-copy-source: bucket/key?versionId=`）/ `UploadPartCopy` / `DeleteObjects` でバージョンを指定できます。有効化前のオブジェクトは `null` バージョンになり、停止（Suspended）中の書き込みは `null` バージョンを置き換えます。削除マーカーを読むと S3 と同じく 404（最新の場合）/ 405（バージョン指定の場合）と `x-amz-delete-marker: true` を返します。古いバージョンを同じキーへコピーすると復元できます。
- **タグ**: オブジェクトは10個、バケットは50個まで。キーは128文字・値は256文字まで、使える文字や `aws:` の予約も S3 と同じく検証し、違反は `InvalidTag` です。CopyObject は `x-amz-tagging-directive: COPY`（既定）/ `REPLACE` に従います。タグ付きのオブジェクトの GET / HEAD は `x-amz-tagging-count` を返します。
- **CORS**: ルールは先頭から評価し、オリジンと `AllowedHeader` の `*`（1つまで）に対応します。preflight はルールがないと 403、一致しないと 403 です。ブラウザから presigned URL で直接アップロードする開発に使えます。
- **エラー**: S3 形式の XML（`Code` / `Message` / `RequestId`）と `x-amz-request-id` を返します。HEAD と 304 は本文なしです。

## 制約

**未対応の機能は黙って無視せず `501 NotImplemented` を返します。** 例: MFA Delete、ACL・ポリシー、SSE、Object Lock、`STANDARD` 以外のストレージクラス、ライフサイクル、通知、`partNumber` 指定の GET。許可リストにない `x-amz-*` ヘッダーやクエリパラメーターも 501 になるため、ローカルでは成功したのに本番では意味が違う、というケースを減らせます。

- 1リクエストの本文は 256 MiB まで（超過は `EntityTooLarge`）。それより大きいオブジェクトはマルチパートで作成します。
- GET は 1 MiB ずつ読み出してストリーミングします。ダウンロード中にオブジェクトが上書き・削除されると、古い内容と新しい内容を混ぜずに接続を打ち切ります（クライアントは短い本文として再試行します）。
- マルチパートは S3 と同じく最後以外のパートが 5 MiB 以上、パート番号 1〜10000 です。完成したオブジェクトにはオブジェクト単位のチェックサムを付けません（パート単位では検証します）。
- CopyObject はコピー元の ETag とチェックサムを引き継ぎます（S3 ではマルチパート由来の ETag が変わる場合があります）。
- us-east-1 のバケットを再作成すると成功し、ほかのリージョンでは `BucketAlreadyOwnedByYou` になります（S3 の挙動に合わせています）。`DeleteBucket` は未完了のマルチパートアップロードを一緒に破棄します。
- バケットを削除するには、削除マーカーを含むすべてのバージョンを先に削除します（S3 と同じ）。
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
# 別ターミナルで LS2 を起動した状態で実行
bash integration/aws-cli.sh              # AWS CLI
(cd integration && go test ./...)        # AWS SDK for Go v2（path-style と virtual-hosted style）
```

GitHub Actions では上記に加えて AWS CLI v2 と Docker イメージのビルドを確認します。

設計・拡張方針は [DESIGN.md](DESIGN.md) を参照してください。
