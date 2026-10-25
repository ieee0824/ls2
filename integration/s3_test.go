package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 5 * 1024 * 1024

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// httpClient dials 127.0.0.1 for `*.localhost`, so virtual-hosted style works even
// where the system resolver does not map subdomains of localhost (RFC 6761).
func httpClient() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err == nil && strings.HasSuffix(host, ".localhost") {
			addr = net.JoinHostPort("127.0.0.1", port)
		}
		return dialer.DialContext(ctx, network, addr)
	}
	return &http.Client{Transport: transport}
}

func newClient(t *testing.T, pathStyle bool) *s3.Client {
	t.Helper()
	endpoint := env("LS2_ENDPOINT", "http://127.0.0.1:9326")
	if !pathStyle {
		endpoint = env("LS2_VHOST_ENDPOINT", "http://localhost:9326")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithHTTPClient(httpClient()),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = pathStyle
	})
}

func TestPathStyle(t *testing.T)          { runScenario(t, newClient(t, true)) }
func TestVirtualHostedStyle(t *testing.T) { runScenario(t, newClient(t, false)) }

func runScenario(t *testing.T, client *s3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	bucket := fmt.Sprintf("ls2-go-%d", time.Now().UnixNano())

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	put := func(key, body string) {
		t.Helper()
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(body),
			ContentType: aws.String("text/plain"),
			Metadata:    map[string]string{"origin": "go"},
		})
		if err != nil {
			t.Fatalf("PutObject(%q): %v", key, err)
		}
	}
	put("docs/a b.txt", "hello")
	put("docs/sub/c.txt", "nested")
	put("top.txt", "top")

	got, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String("docs/a b.txt"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	body, err := io.ReadAll(got.Body)
	got.Body.Close()
	if err != nil || string(body) != "hello" {
		t.Fatalf("GetObject body = %q, %v", body, err)
	}
	if got.Metadata["origin"] != "go" || aws.ToString(got.ContentType) != "text/plain" {
		t.Fatalf("GetObject metadata = %v %v", got.Metadata, got.ContentType)
	}

	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String("docs/"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(listed.Contents) != 1 || aws.ToString(listed.Contents[0].Key) != "docs/a b.txt" ||
		len(listed.CommonPrefixes) != 1 || aws.ToString(listed.CommonPrefixes[0].Prefix) != "docs/sub/" {
		t.Fatalf("ListObjectsV2 = %+v %+v", listed.Contents, listed.CommonPrefixes)
	}

	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("copy.txt"),
		CopySource: aws.String(bucket + "/top.txt"),
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	// Multipart: one full-size part and a short last part.
	data := make([]byte, partSize+1234)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String("big.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var completed []types.CompletedPart
	for i, chunk := range [][]byte{data[:partSize], data[partSize:]} {
		n := int32(i + 1)
		part, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String("big.bin"),
			UploadId: created.UploadId, PartNumber: aws.Int32(n),
			Body: bytes.NewReader(chunk),
		})
		if err != nil {
			t.Fatalf("UploadPart(%d): %v", n, err)
		}
		completed = append(completed, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(n)})
	}
	if _, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String("big.bin"), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	big, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("big.bin")})
	if err != nil {
		t.Fatalf("GetObject(big): %v", err)
	}
	gotBig, err := io.ReadAll(big.Body)
	big.Body.Close()
	if err != nil || !bytes.Equal(gotBig, data) {
		t.Fatalf("multipart round trip differs (%d bytes, %v)", len(gotBig), err)
	}

	// Presigned GET through a plain HTTP client.
	presigned, err := s3.NewPresignClient(client).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("top.txt"),
	})
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	res, err := httpClient().Get(presigned.URL)
	if err != nil {
		t.Fatalf("presigned GET: %v", err)
	}
	presignedBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || string(presignedBody) != "top" {
		t.Fatalf("presigned GET %s = %d %q", presigned.URL, res.StatusCode, presignedBody)
	}

	_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("missing")})
	var noSuchKey *types.NoSuchKey
	if !errors.As(err, &noSuchKey) {
		t.Fatalf("GetObject(missing) error = %v, want NoSuchKey", err)
	}

	var objects []types.ObjectIdentifier
	for _, k := range []string{"docs/a b.txt", "docs/sub/c.txt", "top.txt", "copy.txt", "big.bin"} {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(k)})
	}
	deleted, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects},
	})
	if err != nil || len(deleted.Deleted) != len(objects) {
		t.Fatalf("DeleteObjects = %+v, %v", deleted, err)
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}
