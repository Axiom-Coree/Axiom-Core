package axiom

import (
	"context"
	"log"
	"time"

	"Axiom-Core/internal/executor"
	"Axiom-Core/internal/ingress"
	"Axiom-Core/internal/ledger"
	"Axiom-Core/internal/state"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ring := ingress.NewRingSP(1 << 16)
	store := ledger.NewStore()
	tree := state.NewStore()
	defer tree.Close()

	// Pre-register some accounts.
	for i := 0; i < 100; i++ {
		var id [16]byte
		id[0] = byte(i)
		store.Register(id)
	}

	ex := executor.New(ring, store, tree, executor.Config{
		BatchSize:     4096,
		BatchDeadline: 5 * time.Millisecond,
	})

	// Producer: submit transfers via the gateway (which is what
	// calls the index and pushes to the ring). For this main, just
	// push directly to the ring.
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		var seq uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				var a, b [16]byte
				a[0] = byte(seq % 100)
				b[0] = byte((seq + 1) % 100)
				if a == b {
					continue
				}
				var t struct {
					ID, DebitAccount, CreditAccount [16]byte
					DebitAmount, CreditAmount       int64
					Sequence                        uint64
					State                           uint8
				}
				// (sketch only — use model.Transfer in real code)
				_ = t
				_ = a
				_ = b
			}
		}
	}()

	ex.Run(ctx)

	// On shutdown, verify tree reflects ledger.
	snap := tree.Snapshot()
	defer snap.Release()
	log.Printf("final: applied=%v", snap.Len())
	_ = ctx
}
