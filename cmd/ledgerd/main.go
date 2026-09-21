// Command ledgerd runs the append-only hash-chained event service.
//
// It depends only on the Go standard library.
package main

import (
	"flag"
	"log"
	"net/http"

	"ledgerlab/server"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("dir", "ledgerdata", "ledger data directory")
	flag.Parse()

	srv, err := server.New(*dir)
	if err != nil {
		log.Fatalf("open ledger: %v", err)
	}
	defer srv.Close()

	log.Printf("ledger service listening on %s (dir=%s, chain=%s)", *addr, *dir, srv.Store().ChainID())
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
