// Package s3store implements the blob.Store interface on Amazon S3.
package s3store

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/taskgroup"
	"golang.org/x/time/rate"
)

// Opener constructs a store from an address comprising a bucket name and
// storage region, and an optional prefix, in the format:
//
//	[prefix@]bucket:region
func Opener(_ context.Context, addr string) (blob.Store, error) {
	prefix, bucketRegion, ok := strings.Cut(addr, "@")
	if !ok {
		prefix, bucketRegion = bucketRegion, prefix
	}
	bucket, region, ok := strings.Cut(bucketRegion, ":")
	if !ok {
		return nil, errors.New("invalid S3 address, requires bucket:region")
	}

	return New(bucket, region, &Options{
		// Per https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
		// the rate limit for PUT/DELETE operations is 3500qps per prefix and
		// the rate limit for GET operations is 5500qps per prefix.
		// This appears to include the "empty" prefix for writing the root of the bucket.
		// Moreover, it can take a while for the service to "scale up" to the
		// full load, so here we choose a lower bound. These are the original limits that
		// AWS advertised before the auto-scaling, which doesn't seem to work.
		ReadRateLimit:  rate.NewLimiter(500, 1),
		WriteRateLimit: rate.NewLimiter(300, 1),
		KeyPrefix:      prefix,
	})
}

// A Store implements the blob.Store interface on an S3 bucket.
// Since S3 does not support empty keys, access to an empty key will
// report blob.ErrKeyNotFound as required by the interface.
type Store struct {
	bucket    string
	session   *session.Session
	svc       *s3.S3
	keyPrefix string
	rlimit    Waiter
	wlimit    Waiter
}

// New creates a new Store that references the given bucket and region.
// If opts == nil, default options are provided as described on Options.
//
// By default, New constructs an AWS session using ambient credentials from the
// environment or from a configuration file profile. To specify credentials
// from another source, set the AWSConfig field of the options.
func New(bucket, region string, opts *Options) (*Store, error) {
	sess, err := session.NewSession(opts.awsConfigs(region)...)
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	_, err = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok {
			return nil, err
		}
		switch aerr.Code() {
		case s3.ErrCodeBucketAlreadyExists, s3.ErrCodeBucketAlreadyOwnedByYou:
			// OK, this is our bucket.
		default:
			return nil, err
		}
	}
	return &Store{
		bucket:    bucket,
		session:   sess,
		svc:       svc,
		keyPrefix: opts.keyPrefix(),
		rlimit:    opts.readRateLimit(),
		wlimit:    opts.writeRateLimit(),
	}, nil
}

// Get fetches the contents of a blob from the store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, blob.KeyNotFound(key)
	} else if !s.waitRead(ctx) {
		return nil, fmt.Errorf("rate limit: %w", ctx.Err())
	}

	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.encodeKey(key)),
	})
	if aerr, ok := err.(awserr.Error); ok && isNotFound(aerr.Code()) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

// Put writes a blob to the store.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
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
	awsKey := aws.String(s.encodeKey(opts.Key))
	if _, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    awsKey,
		Body:   bytes.NewReader(opts.Data),
	}); err != nil {
		return err
	}
	return nil
}

// Delete atomically removes a blob from the store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.keyExists(ctx, key); err != nil {
		return err
	}
	if !s.waitWrite(ctx) {
		return fmt.Errorf("rate limit: %w", ctx.Err())
	}
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.encodeKey(key)),
	})
	return err
}

// keyExists returns nil if the specified key exists in the store.
func (s *Store) keyExists(ctx context.Context, key string) error {
	if key == "" {
		return blob.KeyNotFound(key) // S3 does not accept empty keys
	} else if !s.waitRead(ctx) {
		return fmt.Errorf("rate limit: %w", ctx.Err())
	}

	obj, err := s.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    aws.String(s.encodeKey(key)),
	})
	if aerr, ok := err.(awserr.Error); ok && isNotFound(aerr.Code()) {
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
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	req := &s3.ListObjectsV2Input{
		Bucket:     &s.bucket,
		StartAfter: aws.String(s.encodeKey(prevKey(start))),

		// N.B. The S3 API really does mean "after" the selected key, so if we
		// want to use start as a starting point we have to find a key prior to
		// it in the sequence.
	}
	for {
		if !s.waitRead(ctx) {
			return fmt.Errorf("rate limit: %w", ctx.Err())
		}
		pg, err := s.svc.ListObjectsV2(req)
		if err != nil {
			return err
		}
		for _, obj := range pg.Contents {
			key, err := s.decodeKey(*obj.Key)
			if err == errNotMyKey {
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
func (s *Store) Len(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(taskgroup.Trigger(cancel))

	// Note we don't need to check rate limits here, because List already does.
	var total int64
	c := taskgroup.NewCollector(func(v int64) { total += v })
	for i := 0; i < 256; i++ {
		pfx := string([]byte{byte(i)})
		g.Go(c.Task(func() (int64, error) {
			var count int64
			err := s.List(ctx, pfx, func(key string) error {
				if !strings.HasPrefix(key, pfx) {
					return blob.ErrStopListing
				}
				count++
				return nil
			})
			return count, err
		}))
	}
	err := g.Wait()
	c.Wait()

	return total, err
}

// Close implements part of the blob.Store interface. It is a no-op here.
func (*Store) Close(_ context.Context) error { return nil }

// Options are optional settings for a Store. A nil *Options is ready for use
// and provides zero values for all fields.
type Options struct {
	// If set, this prefix is prepended to all keys sent to S3.
	// A trailing slash ("/") is appended if one is not already present.
	KeyPrefix string

	// If set, us this rate limiter for requests to the store.
	ReadRateLimit  Waiter
	WriteRateLimit Waiter

	// An optional AWS configuration to use when constructing the session.
	AWSConfig *aws.Config
}

// A Waiter blocks until a value is available or its context ends.
type Waiter interface {
	// Wait blocks until a value is available or ctx ends.
	// It returns nil if a value is available, otherwise an error.
	Wait(context.Context) error
}

func (o *Options) awsConfigs(region string) []*aws.Config {
	base := aws.NewConfig().WithRegion(region)
	if o == nil || o.AWSConfig == nil {
		return []*aws.Config{base}
	}
	return []*aws.Config{base, o.AWSConfig}
}

func (o *Options) keyPrefix() string {
	if o == nil || o.KeyPrefix == "" {
		return ""
	}
	return o.KeyPrefix
}

func (o *Options) readRateLimit() Waiter {
	if o == nil {
		return nil
	}
	return o.ReadRateLimit
}

func (o *Options) writeRateLimit() Waiter {
	if o == nil {
		return nil
	}
	return o.WriteRateLimit
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

func (s *Store) encodeKey(key string) string {
	tail := hex.EncodeToString([]byte(key))
	if n := len(tail); n < 4 {
		tail += "----"[n:] // ensure _ in prefix/xxx/_ is never empty and sorts early
	}
	return path.Join(s.keyPrefix, tail[:3], tail[3:])
}

func (s *Store) waitWrite(ctx context.Context) bool {
	if s.wlimit == nil {
		return true
	}
	return s.wlimit.Wait(ctx) == nil
}

func (s *Store) waitRead(ctx context.Context) bool {
	if s.rlimit == nil {
		return true
	}
	return s.rlimit.Wait(ctx) == nil
}

var errNotMyKey = errors.New("not a blob key")

func (s *Store) decodeKey(ekey string) (string, error) {
	if s.keyPrefix != "" {
		tail, ok := strings.CutPrefix(ekey, s.keyPrefix+"/")
		if !ok {
			return "", errNotMyKey
		}
		ekey = tail
	}
	pre, post, ok := strings.Cut(ekey, "/")
	if !ok || len(pre) != 3 || post == "" {
		return "", errNotMyKey
	}
	key, err := hex.DecodeString(strings.TrimRight(pre+post, "-"))
	if err != nil {
		return "", err
	}
	return string(key), nil
}

func isNotFound(code string) bool {
	switch code {
	case "NotFound", s3.ErrCodeNoSuchKey:
		return true
	}
	return false
}
