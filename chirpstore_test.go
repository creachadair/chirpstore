package chirpstore_test

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"testing"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/peers"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	gocmp "github.com/google/go-cmp/cmp"
)

// Interface satisfaction checks.
var _ blob.Store = chirpstore.Store{}
var _ blob.CAS = chirpstore.CAS{}

var doDebug = flag.Bool("debug", false, "Enable debug logging")

func logPacket(t *testing.T, tag string) chirp.PacketLogger {
	return func(pkt *chirp.Packet, dir chirp.PacketDir) {
		t.Helper()
		t.Logf("%s: [%c] %v", tag, dir, pkt)
	}
}

func newTestService(t *testing.T, bs blob.Store) *chirp.Peer {
	if bs == nil {
		bs = memstore.New()
	}
	svc := chirpstore.NewService(bs, nil)

	loc := peers.NewLocal()
	svc.Register(loc.A)
	if *doDebug {
		loc.A.LogPackets(logPacket(t, "A"))
		loc.B.LogPackets(logPacket(t, "B"))
	}
	t.Cleanup(func() {
		if err := loc.Stop(); err != nil {
			t.Errorf("Server close: %v", err)
		}
	})
	return loc.B
}

func TestStore(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		peer := newTestService(t, nil)
		rs := chirpstore.NewStore(peer, nil)
		storetest.Run(t, rs)
	})
	t.Run("CAS", func(t *testing.T) {
		peer := newTestService(t, nil)
		rs := chirpstore.NewCAS(peer, nil)
		storetest.Run(t, rs)
	})
}

func TestCAS(t *testing.T) {
	mem := blob.NewCAS(memstore.New(), sha1.New)
	peer := newTestService(t, mem)

	// echo "abcde" | shasum -a 1
	const input = "abcde\n"
	const want = "ec11312386ad561674f724b8cca7cf1796e26d1d"

	ctx := context.Background()
	rs := chirpstore.NewCAS(peer, nil)

	t.Run("CASPut", func(t *testing.T) {
		key, err := rs.CASPut(ctx, blob.CASPutOptions{Data: []byte(input)})
		if err != nil {
			t.Errorf("PutCAS(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("PutCAS(%q): got key %q, want %q", input, got, want)
		}
	})

	t.Run("CASKey", func(t *testing.T) {
		key, err := rs.CASKey(ctx, blob.CASPutOptions{Data: []byte(input)})
		if err != nil {
			t.Errorf("CASKey(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("CASKey(%q): got key %q, want %q", input, got, want)
		}
	})

	t.Run("Len", func(t *testing.T) {
		n, err := rs.Len(ctx)
		if err != nil {
			t.Errorf("Len failed: %v", err)
		} else if n != 1 {
			t.Errorf("Len: got %d, want %d", n, 1)
		}
	})
}

func TestSyncKeyer(t *testing.T) {
	peer := newTestService(t, memstore.New().Init(map[string]string{
		"one": "1", "two": "2", "three": "3", "four": "4",
	}))

	ctx := context.Background()
	rs := chirpstore.NewStore(peer, nil)

	t.Run("NoneMissing", func(t *testing.T) {
		if got, err := rs.SyncKeys(ctx, []string{"one", "three", "two"}); err != nil {
			t.Fatalf("SyncKeys: unexpected error: %v", err)
		} else if len(got) != 0 {
			t.Fatalf("SyncKeys: got %q, want empty", got)
		}
	})
	t.Run("SomeMissing", func(t *testing.T) {
		if got, err := rs.SyncKeys(ctx, []string{"one", "three", "five"}); err != nil {
			t.Fatalf("SyncKeys: unexpected error: %v", err)
		} else if diff := gocmp.Diff(got, []string{"five"}); diff != "" {
			t.Fatalf("SyncKeys (-got, +want):\n%s", diff)
		}
	})
}
