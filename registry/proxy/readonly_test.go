package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/distribution/distribution/v3"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// TestProxyBlobStoreGetReadOnly verifies that in read-only mode, a blob Get
// serves content from the remote registry without writing it to local
// storage (distribution/distribution#4670). Pull-through cache population
// must not bypass the maintenance readonly mode.
func TestProxyBlobStoreGetReadOnly(t *testing.T) {
	te := makeTestEnv(t, "foo/bar")
	te.store.readOnly = true

	populate(t, te, 1, 10, 1)
	remoteDigest := te.inRemote[0].Digest

	blob, err := te.store.Get(te.ctx, remoteDigest)
	if err != nil {
		t.Fatal(err)
	}
	if digest.FromBytes(blob) != remoteDigest {
		t.Fatalf("unexpected blob content: got digest %s, want %s", digest.FromBytes(blob), remoteDigest)
	}

	localStats := te.LocalStats()
	if (*localStats)["put"] != 0 {
		t.Errorf("read-only Get wrote to local storage: local put count = %d, want 0", (*localStats)["put"])
	}

	// A second Get must still serve content (re-fetched from remote since
	// nothing was cached) without writing locally.
	blob, err = te.store.Get(te.ctx, remoteDigest)
	if err != nil {
		t.Fatal(err)
	}
	if digest.FromBytes(blob) != remoteDigest {
		t.Fatalf("unexpected blob content on second Get")
	}
	if (*localStats)["put"] != 0 {
		t.Errorf("read-only Get wrote to local storage on second call: local put count = %d, want 0", (*localStats)["put"])
	}
}

// TestProxyBlobStoreServeBlobReadOnly verifies that in read-only mode, a
// ServeBlob serves content from the remote registry without caching it
// locally (distribution/distribution#4670).
func TestProxyBlobStoreServeBlobReadOnly(t *testing.T) {
	te := makeTestEnv(t, "foo/bar")
	te.store.readOnly = true

	populate(t, te, 1, 10, 1)
	remoteDigest := te.inRemote[0].Digest

	w := httptest.NewRecorder()
	r, err := http.NewRequest(http.MethodGet, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := te.store.ServeBlob(te.ctx, w, r, remoteDigest); err != nil {
		t.Fatal(err)
	}
	resp := w.Result()
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if digest.FromBytes(bodyBytes) != remoteDigest {
		t.Fatalf("unexpected blob content: got digest %s, want %s", digest.FromBytes(bodyBytes), remoteDigest)
	}

	localStats := te.LocalStats()
	if (*localStats)["create"] != 0 {
		t.Errorf("read-only ServeBlob wrote to local storage: local create count = %d, want 0", (*localStats)["create"])
	}

	// The blob must not be present in local storage after the read.
	if _, err := te.store.localStore.Stat(te.ctx, remoteDigest); err != distribution.ErrBlobUnknown {
		t.Errorf("blob unexpectedly present in local storage after read-only ServeBlob: err = %v", err)
	}
}

// TestProxyManifestStoreGetReadOnly verifies that in read-only mode, a
// manifest Get serves content from the remote registry without caching it
// locally (distribution/distribution#4670).
func TestProxyManifestStoreGetReadOnly(t *testing.T) {
	env := newManifestStoreTestEnv(t, "foo/bar", "latest")
	env.manifests.readOnly = true

	ctx := context.Background()
	manifest, err := env.manifests.Get(ctx, env.manifestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if manifest == nil {
		t.Fatal("expected manifest, got nil")
	}

	localStats := env.LocalStats()
	if (*localStats)["put"] != 0 {
		t.Errorf("read-only manifest Get wrote to local storage: local put count = %d, want 0", (*localStats)["put"])
	}
}

// TestProxyTagServiceGetReadOnly verifies that in read-only mode, a tag Get
// returns the remote tag association without caching it locally
// (distribution/distribution#4670).
func TestProxyTagServiceGetReadOnly(t *testing.T) {
	remoteDesc := v1.Descriptor{Digest: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Size: 42}
	proxyTags := testProxyTagService(nil, map[string]v1.Descriptor{"latest": remoteDesc})
	proxyTags.readOnly = true

	ctx := context.Background()
	desc, err := proxyTags.Get(ctx, "latest")
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest != remoteDesc.Digest {
		t.Fatalf("unexpected descriptor: got %s, want %s", desc.Digest, remoteDesc.Digest)
	}

	localTags := proxyTags.localTags.(*mockTagStore)
	if _, ok := localTags.mapping["latest"]; ok {
		t.Errorf("read-only tag Get cached the tag association locally")
	}
}
