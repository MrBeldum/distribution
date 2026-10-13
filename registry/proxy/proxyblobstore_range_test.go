package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestProxyBlobStoreServeBlobRangeCacheMiss is the regression test for
// distribution/distribution#4667: a ranged GET on a blob that is NOT yet in
// the local pull-through cache must return 206 Partial Content with only the
// requested byte range — not 200 with the full blob. The local cache must
// still receive the full blob.
func TestProxyBlobStoreServeBlobRangeCacheMiss(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100) // 1000 bytes

	tests := []struct {
		name        string
		rangeHeader string
		wantStatus  int
		wantRange   string // expected Content-Range header, "" for none
		wantBody    []byte
	}{
		{
			name:        "mid-range",
			rangeHeader: "bytes=100-199",
			wantStatus:  http.StatusPartialContent,
			wantRange:   "bytes 100-199/1000",
			wantBody:    content[100:200],
		},
		{
			name:        "open-ended",
			rangeHeader: "bytes=900-",
			wantStatus:  http.StatusPartialContent,
			wantRange:   "bytes 900-999/1000",
			wantBody:    content[900:],
		},
		{
			name:        "suffix",
			rangeHeader: "bytes=-100",
			wantStatus:  http.StatusPartialContent,
			wantRange:   "bytes 900-999/1000",
			wantBody:    content[900:],
		},
		{
			name:        "full-range",
			rangeHeader: "bytes=0-999",
			wantStatus:  http.StatusPartialContent,
			wantRange:   "bytes 0-999/1000",
			wantBody:    content,
		},
		{
			name:        "no-range",
			rangeHeader: "",
			wantStatus:  http.StatusOK,
			wantRange:   "",
			wantBody:    content,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := makeTestEnv(t, "foo/bar")
			desc, err := te.store.remoteStore.Put(te.ctx, "application/octet-stream", content)
			if err != nil {
				t.Fatalf("Put in remote store: %v", err)
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.rangeHeader != "" {
				r.Header.Set("Range", tt.rangeHeader)
			}

			if err := te.store.ServeBlob(te.ctx, w, r, desc.Digest); err != nil {
				t.Fatalf("ServeBlob: %v", err)
			}

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("reading body: %v", err)
			}

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d (body length %d)", resp.StatusCode, tt.wantStatus, len(body))
			}
			if got := resp.Header.Get("Content-Range"); got != tt.wantRange {
				t.Errorf("Content-Range = %q, want %q", got, tt.wantRange)
			}
			if !bytes.Equal(body, tt.wantBody) {
				t.Errorf("body = %d bytes, want %d bytes", len(body), len(tt.wantBody))
			}

			// The local cache must hold the FULL blob even when the
			// client only asked for a range.
			cached, err := te.store.localStore.Get(te.ctx, desc.Digest)
			if err != nil {
				t.Fatalf("local cache Get: %v", err)
			}
			if !bytes.Equal(cached, content) {
				t.Errorf("cached blob = %d bytes, want full %d bytes", len(cached), len(content))
			}
		})
	}
}

// TestProxyBlobStoreServeBlobRangeCacheHit documents that a ranged GET on an
// already-cached blob keeps returning 206 via the local ServeContent path.
func TestProxyBlobStoreServeBlobRangeCacheHit(t *testing.T) {
	te := makeTestEnv(t, "foo/bar")
	content := bytes.Repeat([]byte("0123456789"), 100) // 1000 bytes
	desc, err := te.store.remoteStore.Put(te.ctx, "application/octet-stream", content)
	if err != nil {
		t.Fatalf("Put in remote store: %v", err)
	}

	// Warm the cache with a full request.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if err := te.store.ServeBlob(te.ctx, w, r, desc.Digest); err != nil {
		t.Fatalf("ServeBlob (warm): %v", err)
	}

	// Ranged request on the now-cached blob.
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Range", "bytes=100-199")
	if err := te.store.ServeBlob(te.ctx, w, r, desc.Digest); err != nil {
		t.Fatalf("ServeBlob (ranged): %v", err)
	}
	resp := w.Result()
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusPartialContent)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 100-199/1000" {
		t.Errorf("Content-Range = %q, want %q", got, "bytes 100-199/1000")
	}
	if !bytes.Equal(body, content[100:200]) {
		t.Errorf("body = %d bytes, want 100", len(body))
	}
}
