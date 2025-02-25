// Package s3store implements the [blob.StoreCloser] interface on Amazon S3.
package s3store

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/hexkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
)

// Opener constructs a store from an address comprising a bucket name and
// storage region, and an optional prefix, in the format:
//
//	[prefix@]bucket:region[?query]
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	prefix, bucketRegion, ok := strings.Cut(addr, "@")
	if !ok {
		prefix, bucketRegion = bucketRegion, prefix
	}
	bucket, region, ok := strings.Cut(bucketRegion, ":")
	if !ok {
		return nil, errors.New("invalid S3 address, requires bucket:region")
	}
	return New(bucket, region, &Options{KeyPrefix: prefix})
}

// Store implements the [blob.StoreCloser] interface over an S3 bucket.
type Store struct {
	*monitor.M[dbState, KV]
}

type dbState struct {
	client *s3.Client
	bucket string
	key    hexkey.Config
}

func (d dbState) addName(name string) dbState { d.key = addName(d.key, name); return d }

func addName(key hexkey.Config, name string) hexkey.Config {
	return key.WithPrefix(path.Join(key.Prefix, "_"+hex.EncodeToString([]byte(name))))
}

// Close implements a method of [blob.StoreCloser].
// This implementation always reports nil.
func (s Store) Close(context.Context) error { return nil }

// New creates a new [Store] that references the given bucket and region.
// If opts == nil, default options are provided as described on [Options].
//
// By default, New constructs an AWS session using ambient credentials from the
// environment or from a configuration file profile. To specify credentials
// from another source, set the [Options.AWSConfigOptions] field.
func New(bucket, region string, opts *Options) (Store, error) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, opts.awsOptions(region)...)
	if err != nil {
		return Store{}, fmt.Errorf("load AWS config: %w", err)
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
			return Store{}, fmt.Errorf("create bucket %q: %w", bucket, err)
		}
	}
	return Store{M: monitor.New(monitor.Config[dbState, KV]{
		DB: dbState{
			client: cli,
			bucket: bucket,
			key:    hexkey.Config{Prefix: opts.keyPrefix(), Shard: 3},
		},
		NewKV: func(_ context.Context, db dbState, _ dbkey.Prefix, name string) (KV, error) {
			return KV{client: db.client, bucket: db.bucket, key: addName(db.key, name)}, nil
		},
		NewSub: func(_ context.Context, db dbState, _ dbkey.Prefix, name string) (dbState, error) {
			return db.addName(name), nil
		},
	})}, nil
}

// A KV implements the [blob.KV] interface on an S3 bucket.
// Since S3 does not support empty keys, access to an empty key will
// report [blob.ErrKeyNotFound] as required by the interface.
type KV struct {
	client *s3.Client
	bucket string
	key    hexkey.Config
}

// Get fetches the contents of a blob from the store.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, blob.KeyNotFound(key)
	}

	obj, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    value.Ptr(s.key.Encode(key)),
	})
	if isNotExist(err) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

// Has implements a method of the [blob.KV] interface.
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	var out blob.KeySet
	for _, key := range keys {
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    value.Ptr(s.key.Encode(key)),
		})
		if isNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		out.Add(key)
	}
	return out, nil
}

// Put writes a blob to the store.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.KeyNotFound(opts.Key)
	} else if !opts.Replace {
		if s.keyExists(ctx, opts.Key) == nil {
			return blob.KeyExists(opts.Key)
		}
	}

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    value.Ptr(s.key.Encode(opts.Key)),
		Body:   bytes.NewReader(opts.Data),
	}); err != nil {
		return err
	}
	return nil
}

// Delete atomically removes a blob from the store.
func (s KV) Delete(ctx context.Context, key string) error {
	if err := s.keyExists(ctx, key); err != nil {
		return err
	}

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    value.Ptr(s.key.Encode(key)),
	})
	return err
}

// keyExists returns nil if the specified key exists in the store.
func (s KV) keyExists(ctx context.Context, key string) error {
	if key == "" {
		return blob.KeyNotFound(key) // S3 does not accept empty keys
	}

	obj, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    value.Ptr(s.key.Encode(key)),
	})
	if isNotExist(err) {
		return blob.KeyNotFound(key)
	} else if err != nil {
		return err
	}
	if value.At(obj.DeleteMarker) {
		return blob.KeyNotFound(key)
	}
	return nil
}

// List calls f with each key in the store in lexicographic order, beginning
// with the first key greater than or equal to start.
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		req := &s3.ListObjectsV2Input{
			Bucket:     &s.bucket,
			StartAfter: value.Ptr(s.key.Start(prevKey(start))),

			// N.B. The S3 API really does mean "after" the selected key, so if we
			// want to use start as a starting point we have to find a key prior to
			// it in the sequence.
		}
		for {
			pg, err := s.client.ListObjectsV2(ctx, req)
			if err != nil {
				yield("", err)
				return
			}
			for _, obj := range pg.Contents {
				key, err := s.key.Decode(*obj.Key)
				if errors.Is(err, hexkey.ErrNotMyKey) {
					continue // some other key in this bucket; ignore it
				} else if err != nil {
					yield("", err)
					return
				}
				if key < start {
					continue
				}
				if !yield(key, nil) {
					return
				}
			}
			if pg.NextContinuationToken == nil {
				break
			}
			req.ContinuationToken = pg.NextContinuationToken
		}
	}
}

// Len reports the number of keys currently in the store.
func (s KV) Len(ctx context.Context) (int64, error) {
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
			for key, err := range s.List(ctx, pfx) {
				if err != nil {
					return 0, err
				} else if !strings.HasPrefix(key, pfx) {
					break
				}
				count++
			}
			return count, nil
		})
	}
	err := g.Wait()
	return total, err
}

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
