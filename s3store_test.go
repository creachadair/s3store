package s3store_test

import (
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
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

func configOrSkip(t *testing.T) *aws.Config {
	t.Helper()

	// N.B. We do not use environment credentials so that we don't accidentally
	// pick up ambient production credentials when running a test. The test has
	// to explicitly specify the values.
	parts := strings.SplitN(*awsCreds, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		t.Skip("Skipping test because credentials are not set")
	}
	creds := credentials.NewStaticCredentials(parts[0], parts[1], "")
	return aws.NewConfig().WithCredentials(creds)
}

func storeOrSkip(t *testing.T, prefix string) *s3store.Store {
	t.Helper()
	cfg := configOrSkip(t)

	t.Logf("Creating store client for bucket %q", *bucketName)
	s, err := s3store.New(*bucketName, *bucketRegion, &s3store.Options{
		ReadQPS:   3000,
		WriteQPS:  1000,
		AWSConfig: cfg,
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return s
}

func TestProbe(t *testing.T) {
	s := storeOrSkip(t, "testprobe")

	const testKey = "test probe key"
	ctx := context.Background()
	err := s.Put(ctx, blob.PutOptions{
		Key:     testKey,
		Data:    []byte("This is a blob to manually verify the store settings.\n"),
		Replace: false,
	})
	if errors.Is(err, blob.ErrKeyExists) {
		t.Logf("Put failed: %v", err)
	} else if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	if err := s.Delete(ctx, testKey); err != nil {
		t.Errorf("Delete %q failed: %v", testKey, err)
	}
}

func TestStoreManual(t *testing.T) {
	s := storeOrSkip(t, "testdata")

	start := time.Now()
	storetest.Run(t, s)
	t.Logf("Store tests completed in %v", time.Since(start))
}
