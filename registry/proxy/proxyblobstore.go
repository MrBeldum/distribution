package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/registry/proxy/scheduler"
	"github.com/distribution/reference"
)

type proxyBlobStore struct {
	localStore        distribution.BlobStore
	remoteStore       distribution.BlobService
	scheduler         *scheduler.TTLExpirationScheduler
	ttl               *time.Duration
	cacheWriteTimeout time.Duration
	repositoryName    reference.Named
	authChallenger    authChallenger
}

var _ distribution.BlobStore = &proxyBlobStore{}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]struct{})

// mu protects inflight
var mu sync.Mutex

func setResponseHeaders(h http.Header, length int64, mediaType string, digest digest.Digest) {
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	h.Set("Content-Type", mediaType)
	h.Set("Docker-Content-Digest", digest.String())
	h.Set("Etag", digest.String())
}

// parseRange parses a single-byte-range Range header (RFC 9110 Section 14.1.2)
// against a representation of the given size. It returns the inclusive
// [start, end] byte range. It reports ok=false when the header is absent,
// malformed, requests multiple ranges, or is unsatisfiable; callers fall back
// to serving the full representation (the pre-existing behavior).
func parseRange(r *http.Request, size int64) (start, end int64, ok bool) {
	const prefix = "bytes="
	spec, found := strings.CutPrefix(r.Header.Get("Range"), prefix)
	if !found {
		return 0, 0, false
	}
	if strings.Contains(spec, ",") {
		// Multiple ranges are not supported; serve the full body.
		return 0, 0, false
	}
	startStr, endStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	if startStr == "" {
		// Suffix range: the last N bytes.
		suffix, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if endStr == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
		if end >= size {
			end = size - 1
		}
	}
	if start >= size {
		// Unsatisfiable range.
		return 0, 0, false
	}
	return start, end, true
}

// rangeWriter passes through only the bytes in the inclusive [start, end]
// window of the stream, discarding the rest. It lets a single remote fetch
// serve a ranged response to the client while the full stream continues to
// another writer (e.g. the local cache).
type rangeWriter struct {
	w     io.Writer
	start int64 // first byte to pass through (inclusive)
	end   int64 // last byte to pass through (inclusive)
	pos   int64 // number of stream bytes seen so far
}

func (rw *rangeWriter) Write(p []byte) (int, error) {
	lo := rw.pos
	hi := rw.pos + int64(len(p)) - 1
	rw.pos += int64(len(p))
	if hi < rw.start || lo > rw.end {
		// Entire chunk outside the window: discard, report consumed.
		return len(p), nil
	}
	s := max(rw.start-lo, 0)
	e := min(rw.end-lo, int64(len(p))-1)
	if _, err := rw.w.Write(p[s : e+1]); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (pbs *proxyBlobStore) copyContent(ctx context.Context, dgst digest.Digest, w http.ResponseWriter, r *http.Request, cacheWriter io.Writer) (v1.Descriptor, error) {
	desc, err := pbs.remoteStore.Stat(ctx, dgst)
	if err != nil {
		return v1.Descriptor{}, err
	}

	setResponseHeaders(w.Header(), desc.Size, desc.MediaType, dgst)

	// Honor a single-byte-range Range header, if present and valid.
	// Without this, ranged requests on a cache miss were answered with
	// 200 and the full blob.
	var rng *struct{ start, end int64 }
	if start, end, ok := parseRange(r, desc.Size); ok {
		rng = &struct{ start, end int64 }{start, end}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, desc.Size))
		w.Header().Set("Accept-Ranges", "bytes")
	}

	remoteReader, err := pbs.remoteStore.Open(ctx, dgst)
	if err != nil {
		return v1.Descriptor{}, err
	}

	defer remoteReader.Close()

	if rng != nil {
		// Send 206 only after the remote stream opened successfully, so a
		// failed open can still surface as an error response.
		w.WriteHeader(http.StatusPartialContent)
	}

	var writer io.Writer = w
	if rng != nil {
		writer = &rangeWriter{w: w, start: rng.start, end: rng.end}
	}
	if cacheWriter != nil {
		writer = io.MultiWriter(writer, cacheWriter)
	}

	_, err = io.CopyN(writer, remoteReader, desc.Size)
	if err != nil {
		return v1.Descriptor{}, err
	}

	proxyMetrics.BlobPull(uint64(desc.Size))
	proxyMetrics.BlobPush(uint64(desc.Size), false)

	return desc, nil
}

func (pbs *proxyBlobStore) serveLocal(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	localDesc, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		// Stat can report a zero sized file here if it's checked between creation
		// and population.  Return nil error, and continue
		return false, nil
	}

	proxyMetrics.BlobPush(uint64(localDesc.Size), true)
	return true, pbs.localStore.ServeBlob(ctx, w, r, dgst)
}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	served, err := pbs.serveLocal(ctx, w, r, dgst)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error serving blob from local storage: %s", err.Error())
		return err
	}

	if served {
		return nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return err
	}

	mu.Lock()
	_, ok := inflight[dgst]
	if ok {
		// If the blob has been serving in other requests.
		// Will return the blob from the remote store directly.
		// TODO Maybe we could reuse the these blobs are serving remotely and caching locally.
		mu.Unlock()
		_, err := pbs.copyContent(ctx, dgst, w, r, nil)
		return err
	}
	inflight[dgst] = struct{}{}
	mu.Unlock()

	defer func() {
		mu.Lock()
		delete(inflight, dgst)
		mu.Unlock()
	}()

	// Create a detached context for the blob writer that won't be canceled
	// when the HTTP request context is canceled. This allows the cache write
	// to complete even if the client disconnects.
	// Use the configured timeout to prevent hanging operations.
	writerCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pbs.cacheWriteTimeout)
	defer cancel()

	bw, err := pbs.localStore.Create(writerCtx)
	if err != nil {
		return err
	}

	committed := false
	// Ensure the writer is canceled if we return early with an error
	defer func() {
		if !committed {
			if err := bw.Cancel(writerCtx); err != nil {
				dcontext.GetLogger(ctx).WithError(err).Errorf("Error canceling blob writer")
			}
		}
	}()

	// Serving client and storing locally over same fetching request.
	// This can prevent a redundant blob fetching.
	desc, err := pbs.copyContent(ctx, dgst, w, r, bw)
	if err != nil {
		return err
	}

	_, err = bw.Commit(writerCtx, desc)
	if err != nil {
		return err
	}

	committed = true

	blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error creating reference: %s", err)
		return err
	}

	if pbs.scheduler != nil && pbs.ttl != nil {
		if err := pbs.scheduler.AddBlob(blobRef, *pbs.ttl); err != nil {
			dcontext.GetLogger(ctx).Errorf("Error adding blob: %s", err)
			return err
		}
	}

	return nil
}

func (pbs *proxyBlobStore) Stat(ctx context.Context, dgst digest.Digest) (v1.Descriptor, error) {
	desc, err := pbs.localStore.Stat(ctx, dgst)
	if err == nil {
		return desc, err
	}

	if err != distribution.ErrBlobUnknown {
		return v1.Descriptor{}, err
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return v1.Descriptor{}, err
	}

	return pbs.remoteStore.Stat(ctx, dgst)
}

func (pbs *proxyBlobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	blob, err := pbs.localStore.Get(ctx, dgst)
	if err == nil {
		return blob, nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return []byte{}, err
	}

	blob, err = pbs.remoteStore.Get(ctx, dgst)
	if err != nil {
		return []byte{}, err
	}

	_, err = pbs.localStore.Put(ctx, "", blob)
	if err != nil {
		return []byte{}, err
	}
	return blob, nil
}

// Unsupported functions
func (pbs *proxyBlobStore) Put(ctx context.Context, mediaType string, p []byte) (v1.Descriptor, error) {
	return v1.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest) (v1.Descriptor, error) {
	return v1.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Open(ctx context.Context, dgst digest.Digest) (io.ReadSeekCloser, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return distribution.ErrUnsupported
}
