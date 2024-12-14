// Package s3store implements the [blob.KV] interface on Amazon S3.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/hexkey"
	"github.com/creachadair/taskgroup"
	"golang.org/x/time/rate"
)

// Opener constructs a store from an address comprising a bucket name and
// storage region, and an optional prefix, in the format:
//
//	[prefix@]bucket:region[?query]
//
// Query parameters:
//
//	read_qps   : rate limit for read (GET) queries
//	write_qps  : rate limit for write (PUT/DELETE) queries
func Opener(_ context.Context, addr string) (blob.KV, error) {
	prefix, bucketRegion, ok := strings.Cut(addr, "@")
	if !ok {
		prefix, bucketRegion = bucketRegion, prefix
	}
	bucket, region, ok := strings.Cut(bucketRegion, ":")
	if !ok {
		return nil, errors.New("invalid S3 address, requires bucket:region")
	}
	opts := &Options{
		// Per https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
		// the rate limit for PUT/DELETE operations is 3500qps per prefix and
		// the rate limit for GET operations is 5500qps per prefix.
		// This appears to include the "empty" prefix for writing the root of the bucket.
		// Moreover, it can take a while for the service to "scale up" to the full rate.
		// Default to no limit.
		ReadQPS:   0,
		WriteQPS:  0,
		KeyPrefix: prefix,
	}
	if base, query, ok := strings.Cut(region, "?"); ok {
		region = base
		q, err := url.ParseQuery(query)
		if err != nil {
			return nil, fmt.Errorf("invalid query: %w", err)
		}
		if v, ok := getQueryInt(q, "read_qps"); ok {
			opts.ReadQPS = v
		}
		if v, ok := getQueryInt(q, "write_qps"); ok {
			opts.WriteQPS = v
		}
	}
	return New(bucket, region, opts)
}

// A KV implements the [blob.KV] interface on an S3 bucket.
// Since S3 does not support empty keys, access to an empty key will
// report [blob.ErrKeyNotFound] as required by the interface.
type KV struct {
	bucket   string
	key      hexkey.Config
	s3Client *s3.Client
	rlimit   waiter
	wlimit   waiter
}

// New creates a new [kV] that references the given bucket and region.
// If opts == nil, default options are provided as described on [Options].
//
// By default, New constructs an AWS session using ambient credentials from the
// environment or from a configuration file profile. To specify credentials
// from another source, set the [Options.AWSConfigOptions] field.
func New(bucket, region string, opts *Options) (*KV, error) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, opts.awsOptions(region)...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	cli := s3.NewFromConfig(cfg)
	_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,

		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	if err != nil {
		var e1 *types.BucketAlreadyExists
		var e2 *types.BucketAlreadyOwnedByYou
		if errors.As(err, &e1) || errors.As(err, &e2) {
			// OK, this is our bucket
		} else {
			return nil, fmt.Errorf("create bucket %q: %w", bucket, err)
		}
	}
	return &KV{
		bucket:   bucket,
		s3Client: cli,
		key:      hexkey.Config{Prefix: opts.keyPrefix(), Shard: 3},
		rlimit:   opts.readRateLimit(),
		wlimit:   opts.writeRateLimit(),
	}, nil
}

// Get fetches the contents of a blob from the store.
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, blob.KeyNotFound(key)
	} else if !s.waitRead(ctx) {
		return nil, fmt.Errorf("rate limit: %w", ctx.Err())
	}

	obj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.key.Encode(key)),
	})
	if isNotExist(err) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

// Put writes a blob to the store.
func (s *KV) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.KeyNotFound(opts.Key)
	} else if !opts.Replace {
		if s.keyExists(ctx, opts.Key) == nil {
			return blob.KeyExists(opts.Key)
		}
	}

	if !s.waitWrite(ctx) {
		return fmt.Errorf("rate limit: %w", ctx.Err())
	}
	if _, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.key.Encode(opts.Key)),
		Body:   bytes.NewReader(opts.Data),
	}); err != nil {
		return err
	}
	return nil
}

// Delete atomically removes a blob from the store.
func (s *KV) Delete(ctx context.Context, key string) error {
	if err := s.keyExists(ctx, key); err != nil {
		return err
	}
	if !s.waitWrite(ctx) {
		return fmt.Errorf("rate limit: %w", ctx.Err())
	}
	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.key.Encode(key)),
	})
	return err
}

// keyExists returns nil if the specified key exists in the store.
func (s *KV) keyExists(ctx context.Context, key string) error {
	if key == "" {
		return blob.KeyNotFound(key) // S3 does not accept empty keys
	} else if !s.waitRead(ctx) {
		return fmt.Errorf("rate limit: %w", ctx.Err())
	}

	obj, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.key.Encode(key)),
	})
	if isNotExist(err) {
		return blob.KeyNotFound(key)
	} else if err != nil {
		return err
	}
	if obj.DeleteMarker != nil && *obj.DeleteMarker {
		return blob.KeyNotFound(key)
	}
	return nil
}

// List calls f with each key in the store in lexicographic order, beginning
// with the first key greater than or equal to start.
func (s *KV) List(ctx context.Context, start string, f func(string) error) error {
	req := &s3.ListObjectsV2Input{
		Bucket:     &s.bucket,
		StartAfter: aws.String(s.key.Encode(prevKey(start))),

		// N.B. The S3 API really does mean "after" the selected key, so if we
		// want to use start as a starting point we have to find a key prior to
		// it in the sequence.
	}
	for {
		if !s.waitRead(ctx) {
			return fmt.Errorf("rate limit: %w", ctx.Err())
		}
		pg, err := s.s3Client.ListObjectsV2(ctx, req)
		if err != nil {
			return err
		}
		for _, obj := range pg.Contents {
			key, err := s.key.Decode(*obj.Key)
			if errors.Is(err, hexkey.ErrNotMyKey) {
				continue // some other key in this bucket; ignore it
			} else if err != nil {
				return err
			}
			if key < start {
				continue
			}
			if err := f(key); err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
		if pg.NextContinuationToken == nil {
			break
		}
		req.ContinuationToken = pg.NextContinuationToken
	}
	return nil
}

// Len reports the number of keys currently in the store.
func (s *KV) Len(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)

	// Note we don't need to check rate limits here, because List already does.
	var total int64
	c := taskgroup.Gather(g.Go, func(v int64) { total += v })
	for i := 0; i < 256; i++ {
		pfx := string([]byte{byte(i)})
		c.Call(func() (int64, error) {
			var count int64
			err := s.List(ctx, pfx, func(key string) error {
				if !strings.HasPrefix(key, pfx) {
					return blob.ErrStopListing
				}
				count++
				return nil
			})
			return count, err
		})
	}
	err := g.Wait()
	return total, err
}

// Close implements part of the [blob.KV] interface. It is a no-op here.
func (*KV) Close(_ context.Context) error { return nil }

// Options are optional settings for a [KV]. A nil *Options is ready for use
// and provides zero values for all fields.
type Options struct {
	// If set, this prefix is prepended to all keys sent to S3.
	// A trailing slash ("/") is appended if one is not already present.
	KeyPrefix string

	// If set, us these rate limits apply to requests to the store.
	ReadQPS  int
	WriteQPS int

	// Optional AWS config loader options.
	AWSConfigOptions []func(*config.LoadOptions) error
}

// A waiter blocks until a value is available or its context ends.
type waiter interface {
	// Wait blocks until a value is available or ctx ends.
	// It returns nil if a value is available, otherwise an error.
	Wait(context.Context) error
}

func (o *Options) awsOptions(region string) (out []func(*config.LoadOptions) error) {
	if region != "" {
		out = append(out, config.WithRegion(region))
	}
	if o != nil {
		out = append(out, o.AWSConfigOptions...)
	}
	return out
}

func (o *Options) keyPrefix() string {
	if o == nil || o.KeyPrefix == "" {
		return ""
	}
	return o.KeyPrefix
}

func (o *Options) readRateLimit() waiter {
	if o == nil || o.ReadQPS <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(o.ReadQPS), 1)
}

func (o *Options) writeRateLimit() waiter {
	if o == nil || o.WriteQPS <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(o.WriteQPS), 1)
}

// prevKey returns the string that is immediately lexicographically prior to
// key, or "" if key == "".
func prevKey(key string) string {
	pk := []byte(key)
	for i := len(pk) - 1; i >= 0; i-- {
		if pk[i] > 0 {
			pk[i]--
			break
		}
		pk[i] = 255
		if i == 0 {
			pk = pk[:len(pk)-1]
		}
	}
	return string(pk)
}

func (s *KV) waitWrite(ctx context.Context) bool {
	if s.wlimit == nil {
		return true
	}
	return s.wlimit.Wait(ctx) == nil
}

func (s *KV) waitRead(ctx context.Context) bool {
	if s.rlimit == nil {
		return true
	}
	return s.rlimit.Wait(ctx) == nil
}

func getQueryInt(q url.Values, name string) (int, bool) {
	if !q.Has(name) {
		return 0, false
	} else if v, err := strconv.Atoi(q.Get(name)); err == nil {
		return v, true
	}
	return 0, false
}

// isNotExist reports whether err is an error indicating the requested resource
// was not found, taking into account S3 and standard library types.
func isNotExist(err error) bool {
	var e1 *types.NotFound
	var e2 *types.NoSuchKey
	if errors.As(err, &e1) || errors.As(err, &e2) {
		return true
	}
	return errors.Is(err, os.ErrNotExist)
}
