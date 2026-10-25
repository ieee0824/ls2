#!/usr/bin/env bash
# End-to-end check with the AWS CLI against a running ls2 (`cargo run --locked`).
set -euo pipefail

endpoint=${LS2_ENDPOINT:-http://127.0.0.1:9326}
# Dummy credentials: ls2 does not verify signatures.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
s3() { aws --endpoint-url "$endpoint" "$@"; }

work=$(mktemp -d)
bucket="ls2-it-$(date +%s)-$RANDOM"
trap 'rm -rf "$work"' EXIT

step() { printf '== %s\n' "$*"; }

step "health"
curl --fail --silent "$endpoint/_ls2/health" >/dev/null

step "make bucket"
s3 s3 mb "s3://$bucket"

step "single upload / download"
printf 'hello ls2\n' >"$work/small.txt"
s3 s3 cp --only-show-errors "$work/small.txt" "s3://$bucket/dir/small.txt"
s3 s3 cp --only-show-errors "s3://$bucket/dir/small.txt" "$work/small.out"
cmp "$work/small.txt" "$work/small.out"

step "multipart upload / ranged download (20 MiB)"
head -c 20971520 /dev/urandom >"$work/big.bin"
s3 s3 cp --only-show-errors "$work/big.bin" "s3://$bucket/big.bin"
s3 s3 cp --only-show-errors "s3://$bucket/big.bin" "$work/big.out"
cmp "$work/big.bin" "$work/big.out"

step "server-side copy (UploadPartCopy)"
s3 s3 cp --only-show-errors "s3://$bucket/big.bin" "s3://$bucket/copy/big.bin"
s3 s3 cp --only-show-errors "s3://$bucket/copy/big.bin" "$work/copy.out"
cmp "$work/big.bin" "$work/copy.out"

step "sync and list"
mkdir -p "$work/tree/a/b"
for f in one two a/three a/b/four "a/b/with space"; do printf '%s' "$f" >"$work/tree/$f"; done
s3 s3 sync --only-show-errors "$work/tree" "s3://$bucket/tree"
listing=$(s3 s3 ls --recursive "s3://$bucket/tree/")
[ "$(printf '%s\n' "$listing" | wc -l | tr -d ' ')" = 5 ]
grep -q 'tree/a/b/with space' <<<"$listing"
root=$(s3 s3 ls "s3://$bucket/")
grep -q 'PRE dir/' <<<"$root"
grep -q 'big.bin' <<<"$root"

step "checksums and metadata"
s3 s3api put-object --bucket "$bucket" --key sha.txt --body "$work/small.txt" \
  --checksum-algorithm SHA256 --content-type text/plain --metadata color=blue >/dev/null
head_json=$(s3 s3api head-object --bucket "$bucket" --key sha.txt --checksum-mode ENABLED)
grep -q '"ChecksumSHA256"' <<<"$head_json"
grep -q '"color": "blue"' <<<"$head_json"
s3 s3api get-object --bucket "$bucket" --key sha.txt --checksum-mode ENABLED "$work/sha.out" >/dev/null
cmp "$work/small.txt" "$work/sha.out"

step "conditional write"
if s3 s3api put-object --bucket "$bucket" --key sha.txt --body "$work/small.txt" --if-none-match '*' 2>/dev/null; then
  echo "expected PreconditionFailed" >&2
  exit 1
fi

step "recursive delete and remove bucket"
s3 s3 rm --only-show-errors --recursive "s3://$bucket" >/dev/null
s3 s3 rb "s3://$bucket"

echo "OK"
