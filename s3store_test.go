package s3store_test

import (
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/s3store"
)

var (
	awsCreds = flag.String("credentials", "",
		"AWS access credentials (key:secret)")
	bucketName = flag.String("bucket", "s3store-test-data",
		"Bucket name to use or create for testing")
	bucketRegion = flag.String("region", "us-west-2",
		"AWS region to use for the testing bucket")
)

func configOrSkip(t *testing.T) []func(*config.LoadOptions) error {
	t.Helper()

	// N.B. We do not use environment credentials so that we don't accidentally
	// pick up ambient production credentials when running a test. The test has
	// to explicitly specify the values.
	parts := strings.SplitN(*awsCreds, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		t.Skip("Skipping test because credentials are not set")
	}
	creds := credentials.NewStaticCredentialsProvider(parts[0], parts[1], "" /* session */)
	return []func(*config.LoadOptions) error{config.WithCredentialsProvider(creds)}
}

func storeOrSkip(t *testing.T, prefix string) s3store.Store {
	t.Helper()
	cfg := configOrSkip(t)

	t.Logf("Creating store client for bucket %q", *bucketName)
	s, err := s3store.New(*bucketName, *bucketRegion, &s3store.Options{
		KeyPrefix:        prefix,
		AWSConfigOptions: cfg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return s
}

func TestProbe(t *testing.T) {
	s := storeOrSkip(t, "testprobe")

	ctx := context.Background()
	kv := storetest.SubKeyspace(t, ctx, s, "")

	const testKey = "test probe key"
	const text = "This is a blob to manually verify the store settings.\n"
	err := kv.Put(ctx, blob.PutOptions{
		Key:     testKey,
		Data:    []byte(text),
		Replace: false,
	})
	if errors.Is(err, blob.ErrKeyExists) {
		t.Logf("Put failed: %v", err)
	} else if err != nil {
		t.Errorf("Put failed: %v", err)
	}
	got, err := kv.Get(ctx, testKey)
	if err != nil {
		t.Errorf("Get %q failed: %v", testKey, err)
	} else if s := string(got); s != text {
		t.Errorf("Get %q: got %q, want %q", testKey, s, text)
	}
	if err := kv.Delete(ctx, testKey); err != nil {
		t.Errorf("Delete %q failed: %v", testKey, err)
	}
}

func TestStoreManual(t *testing.T) {
	s := storeOrSkip(t, "testdata")

	start := time.Now()
	storetest.Run(t, s)
	t.Logf("Store tests completed in %v", time.Since(start))
}
