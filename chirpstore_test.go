package chirpstore_test

import (
	"context"
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
var (
	_ blob.KV        = chirpstore.KV{}
	_ blob.SyncKeyer = chirpstore.KV{}
)

var doDebug = flag.Bool("debug", false, "Enable debug logging")

func logPacket(t *testing.T, tag string) chirp.PacketLogger {
	return func(pkt *chirp.Packet, dir chirp.PacketDir) {
		t.Helper()
		t.Logf("%s: [%s] %v", tag, dir, pkt)
	}
}

func newTestService(t *testing.T, bs blob.Store) *chirp.Peer {
	if bs == nil {
		bs = memstore.New(nil)
	}
	svc := chirpstore.NewService(bs, nil)

	loc := peers.NewLocal()
	svc.Register(loc.A)
	if *doDebug {
		loc.A.LogPackets(logPacket(t, "srv"))
	}
	t.Cleanup(func() {
		if err := loc.Stop(); err != nil {
			t.Errorf("Server close: %v", err)
		}
	})
	return loc.B
}

func TestStore(t *testing.T) {
	peer := newTestService(t, nil)
	rs := chirpstore.NewStore(peer, nil)
	storetest.Run(t, rs)
}

func TestCAS(t *testing.T) {
	peer := newTestService(t, nil)
	rs := chirpstore.NewStore(peer, nil)

	ctx := context.Background()
	cas := storetest.SubCAS(t, ctx, rs, "")

	// Generate: https://go.dev/play/p/96wllQpFbRt
	const input = "abcde\n"
	const want = "171f7c7fa25081bd0e52d37990dd2d7624f986af4fc50714680c7f0e9df631d5"

	t.Run("CASPut", func(t *testing.T) {
		key, err := cas.CASPut(ctx, []byte(input))
		if err != nil {
			t.Errorf("PutCAS(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("PutCAS(%q): got key %q, want %q", input, got, want)
		}
	})

	t.Run("CASKey", func(t *testing.T) {
		key := cas.CASKey(ctx, []byte(input))
		if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("CASKey(%q): got key %q, want %q", input, got, want)
		}
	})

	t.Run("Len", func(t *testing.T) {
		n, err := cas.Len(ctx)
		if err != nil {
			t.Errorf("Len failed: %v", err)
		} else if n != 1 {
			t.Errorf("Len: got %d, want %d", n, 1)
		}
	})
}

func TestSyncKeyer(t *testing.T) {
	peer := newTestService(t, memstore.New(func() blob.KV {
		return memstore.NewKV().Init(map[string]string{
			"one": "1", "two": "2", "three": "3", "four": "4",
		})
	}))

	ctx := context.Background()
	rs := chirpstore.NewStore(peer, nil)
	kv := storetest.SubKV(t, ctx, rs, "").(chirpstore.KV)

	t.Run("NoneMissing", func(t *testing.T) {
		if got, err := kv.SyncKeys(ctx, []string{"one", "three", "two"}); err != nil {
			t.Fatalf("SyncKeys: unexpected error: %v", err)
		} else if len(got) != 0 {
			t.Fatalf("SyncKeys: got %q, want empty", got)
		}
	})
	t.Run("SomeMissing", func(t *testing.T) {
		if got, err := kv.SyncKeys(ctx, []string{"one", "three", "five"}); err != nil {
			t.Fatalf("SyncKeys: unexpected error: %v", err)
		} else if diff := gocmp.Diff(got, []string{"five"}); diff != "" {
			t.Fatalf("SyncKeys (-got, +want):\n%s", diff)
		}
	})
}
