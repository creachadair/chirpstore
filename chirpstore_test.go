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
)

// Interface satisfaction checks.
var _ blob.Store = chirpstore.Store{}
var _ blob.CAS = chirpstore.CAS{}

var doDebug = flag.Bool("debug", false, "Enable debug logging")

func TestStore(t *testing.T) {
	mem := memstore.New()
	svc := chirpstore.NewService(mem, nil)

	loc := peers.NewLocal()
	svc.Register(loc.A)
	if *doDebug {
		loc.A.LogPacket(func(pkt *chirp.Packet) { t.Logf("A receive: %v", pkt) })
		loc.B.LogPacket(func(pkt *chirp.Packet) { t.Logf("B receive: %v", pkt) })
	}

	t.Run("Store", func(t *testing.T) {
		rs := chirpstore.NewStore(loc.B, nil)
		storetest.Run(t, rs)
	})
	t.Run("CAS", func(t *testing.T) {
		rs := chirpstore.NewCAS(loc.B, nil)
		storetest.Run(t, rs)
	})

	if err := loc.Stop(); err != nil {
		t.Fatalf("Server close: %v", err)
	}
}

func TestCAS(t *testing.T) {
	mem := blob.NewCAS(memstore.New(), sha1.New)
	svc := chirpstore.NewService(mem, nil)

	loc := peers.NewLocal()
	defer loc.Stop()
	svc.Register(loc.A)

	// echo "abcde" | shasum -a 1
	const input = "abcde\n"
	const want = "ec11312386ad561674f724b8cca7cf1796e26d1d"

	ctx := context.Background()
	rs := chirpstore.NewCAS(loc.B, nil)
	t.Run("CASPut", func(t *testing.T) {
		key, err := rs.CASPut(ctx, []byte(input))
		if err != nil {
			t.Errorf("PutCAS(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("PutCAS(%q): got key %q, want %q", input, got, want)
		}
	})
	t.Run("CASKey", func(t *testing.T) {
		key, err := rs.CASKey(ctx, []byte(input))
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
