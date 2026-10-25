package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// TestNotificationsToLQS needs ls2 started with LS2_SQS_ENDPOINT pointing at lqs,
// and LQS_ENDPOINT set for the test itself.
func TestNotificationsToLQS(t *testing.T) {
	endpoint := env("LQS_ENDPOINT", "")
	if endpoint == "" {
		t.Skip("LQS_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	queues := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
	client := newClient(t, true)

	name := fmt.Sprintf("ls2-events-%d", time.Now().UnixNano())
	created, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	bucket := name
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	missing := &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
		QueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:does-not-exist"),
		Events:   []types.Event{"s3:ObjectCreated:*"},
	}}}
	if _, err := client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket), NotificationConfiguration: missing,
	}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("missing queue: err = %v, want InvalidArgument", err)
	}
	arn := "arn:aws:sqs:us-east-1:000000000000:" + name
	if _, err := client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
			Id:       aws.String("all"),
			QueueArn: aws.String(arn),
			Events:   []types.Event{"s3:ObjectCreated:*", "s3:ObjectRemoved:*", "s3:LifecycleExpiration:*"},
		}}},
	}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}
	got, err := client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil || len(got.QueueConfigurations) != 1 || aws.ToString(got.QueueConfigurations[0].QueueArn) != arn {
		t.Fatalf("GetBucketNotificationConfiguration = %+v, %v", got, err)
	}

	// receive returns the next message body from the queue.
	receive := func() map[string]any {
		t.Helper()
		for ctx.Err() == nil {
			out, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl: created.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 2,
			})
			if err != nil {
				t.Fatalf("ReceiveMessage: %v", err)
			}
			if len(out.Messages) == 0 {
				continue
			}
			m := out.Messages[0]
			if _, err := queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: created.QueueUrl, ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatalf("DeleteMessage: %v", err)
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &body); err != nil {
				t.Fatalf("message body %q: %v", aws.ToString(m.Body), err)
			}
			return body
		}
		t.Fatal("no message before the deadline")
		return nil
	}
	record := func(body map[string]any) (string, string) {
		t.Helper()
		records, _ := body["Records"].([]any)
		if len(records) != 1 {
			t.Fatalf("event = %v", body)
		}
		r := records[0].(map[string]any)
		key := r["s3"].(map[string]any)["object"].(map[string]any)["key"].(string)
		return r["eventName"].(string), key
	}

	if test := receive(); test["Event"] != "s3:TestEvent" {
		t.Fatalf("first message = %v, want s3:TestEvent", test)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("logs/a b.txt"), Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if name, key := record(receive()); name != "ObjectCreated:Put" || key != "logs/a+b.txt" {
		t.Fatalf("event = %s %s", name, key)
	}

	// Lifecycle: expire logs/ after a day, then run the rules as of a future date.
	if _, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
			ID:         aws.String("logs"),
			Status:     types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
		}}},
	}); err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration: %v", err)
	}
	rules, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil || len(rules.Rules) != 1 || aws.ToInt32(rules.Rules[0].Expiration.Days) != 1 {
		t.Fatalf("GetBucketLifecycleConfiguration = %+v, %v", rules, err)
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("logs/a b.txt")})
	if err != nil || !strings.Contains(aws.ToString(head.Expiration), `rule-id="logs"`) {
		t.Fatalf("HeadObject Expiration = %q, %v", aws.ToString(head.Expiration), err)
	}
	ls2 := env("LS2_ENDPOINT", "http://127.0.0.1:9326")
	response, err := http.Post(ls2+"/_ls2/lifecycle?now="+time.Now().Add(72*time.Hour).UTC().Format(time.RFC3339), "", nil)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("run lifecycle: %v %v", response, err)
	}
	response.Body.Close()
	if name, key := record(receive()); name != "LifecycleExpiration:Delete" || key != "logs/a+b.txt" {
		t.Fatalf("event = %s %s", name, key)
	}
	if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("logs/a b.txt")}); err == nil {
		t.Fatal("expired object still exists")
	}

	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := queues.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: created.QueueUrl}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
}
