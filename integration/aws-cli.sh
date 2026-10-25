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

step "virtual-hosted style and browser"
port=${endpoint##*:}
vhost_body=$(curl --fail --silent --resolve "$bucket.localhost:$port:127.0.0.1" \
  "http://$bucket.localhost:$port/dir/small.txt")
[ "$vhost_body" = "hello ls2" ]
page=$(curl --fail --silent "$endpoint/_ls2/browse/$bucket?prefix=dir%2F")
grep -q 'small.txt' <<<"$page"
curl --fail --silent -o "$work/download.out" "$endpoint/_ls2/download/$bucket?key=dir%2Fsmall.txt"
cmp "$work/small.txt" "$work/download.out"

step "conditional write"
if s3 s3api put-object --bucket "$bucket" --key sha.txt --body "$work/small.txt" --if-none-match '*' 2>/dev/null; then
  echo "expected PreconditionFailed" >&2
  exit 1
fi

step "tagging and CORS"
s3 s3api put-object-tagging --bucket "$bucket" --key sha.txt \
  --tagging 'TagSet=[{Key=env,Value=dev}]'
tag_value=$(s3 s3api get-object-tagging --bucket "$bucket" --key sha.txt --query 'TagSet[0].Value' --output text)
[ "$tag_value" = dev ]
s3 s3api put-bucket-cors --bucket "$bucket" --cors-configuration \
  '{"CORSRules":[{"AllowedOrigins":["http://localhost:3000"],"AllowedMethods":["GET"]}]}'
allow_origin=$(curl --fail --silent -o /dev/null -X OPTIONS -w '%header{access-control-allow-origin}' \
  -H 'Origin: http://localhost:3000' -H 'Access-Control-Request-Method: GET' "$endpoint/$bucket/sha.txt")
[ "$allow_origin" = "http://localhost:3000" ]

step "lifecycle"
s3 s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration \
  '{"Rules":[{"ID":"tmp","Status":"Enabled","Filter":{"Prefix":"tmp/"},"Expiration":{"Days":1}}]}'
rule_id=$(s3 s3api get-bucket-lifecycle-configuration --bucket "$bucket" --query 'Rules[0].ID' --output text)
[ "$rule_id" = tmp ]
s3 s3 cp --only-show-errors "$work/small.txt" "s3://$bucket/tmp/expiring.txt"
expiration=$(s3 s3api head-object --bucket "$bucket" --key tmp/expiring.txt --query Expiration --output text)
grep -q 'rule-id="tmp"' <<<"$expiration"
curl --fail --silent -X POST "$endpoint/_ls2/lifecycle?now=2999-01-01T00:00:00Z" >/dev/null
if s3 s3api head-object --bucket "$bucket" --key tmp/expiring.txt >/dev/null 2>&1; then
  echo "expected the lifecycle rule to expire tmp/expiring.txt" >&2
  exit 1
fi
s3 s3api delete-bucket-lifecycle --bucket "$bucket"

step "versioning"
s3 s3api put-bucket-versioning --bucket "$bucket" --versioning-configuration Status=Enabled
printf 'v1' >"$work/v1.txt"
printf 'v2' >"$work/v2.txt"
v1=$(s3 s3api put-object --bucket "$bucket" --key versioned.txt --body "$work/v1.txt" --query VersionId --output text)
s3 s3api put-object --bucket "$bucket" --key versioned.txt --body "$work/v2.txt" >/dev/null
s3 s3 rm --only-show-errors "s3://$bucket/versioned.txt"
s3 s3api get-object --bucket "$bucket" --key versioned.txt --version-id "$v1" "$work/v1.out" >/dev/null
cmp "$work/v1.txt" "$work/v1.out"
versions=$(s3 s3api list-object-versions --bucket "$bucket" --prefix versioned.txt \
  --query '[length(Versions), length(DeleteMarkers)]' --output text)
[ "$(tr -s '[:space:]' ' ' <<<"$versions" | sed 's/ $//')" = "2 1" ]
# Remove every version (and delete marker) so the bucket can be deleted.
s3 s3api list-object-versions --bucket "$bucket" \
  --query '{Objects: [Versions[].{Key: Key, VersionId: VersionId}, DeleteMarkers[].{Key: Key, VersionId: VersionId}][]}' \
  --output json >"$work/delete.json"

step "recursive delete and remove bucket"
s3 s3api delete-objects --bucket "$bucket" --delete "file://$work/delete.json" >/dev/null
remaining=$(s3 s3api list-object-versions --bucket "$bucket" --query 'length(Versions || `[]`)' --output text)
[ "$remaining" = 0 ]
s3 s3 rb "s3://$bucket"

echo "OK"
