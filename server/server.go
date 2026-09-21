// Package server exposes the ledger over HTTP. It is safe for concurrent
// writers, readers and verifiers: the store serialises writes, readers see
// only committed events, and long verification blocks new appends so a slow
// check can never race the tail it is verifying.
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"ledgerlab/ledger"
)

// Server wraps a store with HTTP handlers and a maintenance lock.
type Server struct {
	store *ledger.Store

	// verifyMu makes slow verify/recover operations exclusive with respect to
	// appends: a verifier holds the gate, preventing new writes from advancing
	// the tail mid-check, while concurrent readers remain available.
	verifyMu sync.RWMutex

	// handleMu protects swapping the active store during an explicit recover.
	handleMu sync.RWMutex

	mux *http.ServeMux
}

// New creates a server backed by a ledger opened in dir.
func New(dir string) (*Server, error) {
	st, err := ledger.Open(dir)
	if err != nil {
		return nil, err
	}
	s := &Server{store: st, mux: http.NewServeMux()}
	s.routes()
	return s, nil
}

// NewWithStore wraps an already opened store (used by tests).
func NewWithStore(st *ledger.Store) *Server {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Store exposes the underlying store.
func (s *Server) Store() *ledger.Store { return s.store }

// Handler returns the routing handler.
func (s *Server) Handler() http.Handler { return s.mux }

// Close releases the store.
func (s *Server) Close() error { return s.store.Close() }

func (s *Server) routes() {
	s.mux.HandleFunc("/events", s.withStore(s.handleEvents))
	s.mux.HandleFunc("/events/", s.withStore(s.handleEventBy))
	s.mux.HandleFunc("/verify", s.withStore(s.handleVerify))
	s.mux.HandleFunc("/status", s.withStore(s.handleStatus))
	s.mux.HandleFunc("/recover", s.withStore(s.handleRecover))
}

// withStore pins the active store for the duration of a request so an
// in-flight handler cannot observe a handle swapped by recovery.
func (s *Server) withStore(h func(http.ResponseWriter, *http.Request, *ledger.Store)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.handleMu.RLock()
		st := s.store
		s.handleMu.RUnlock()
		h(w, r, st)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeLedgerError(w http.ResponseWriter, err error) {
	le := &ledger.Error{}
	code := http.StatusInternalServerError
	if errors.As(err, &le) {
		switch le.Kind {
		case ledger.ErrNotFound:
			code = http.StatusNotFound
		case ledger.ErrInvalidEvent, ledger.ErrBatchConflict, ledger.ErrKeyConflict:
			code = http.StatusConflict
			if le.Kind == ledger.ErrInvalidEvent {
				code = http.StatusBadRequest
			}
		case ledger.ErrForeignChain, ledger.ErrCheckpointInvalid,
			ledger.ErrHashMismatch, ledger.ErrSequenceJump, ledger.ErrFrameCorrupt:
			code = http.StatusUnprocessableEntity
		}
		writeJSON(w, code, le)
		return
	}
	writeJSON(w, code, map[string]string{"kind": "internal", "message": err.Error()})
}

// handleEvents serves POST (append batch) and GET (list tail window).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request, store *ledger.Store) {
	switch r.Method {
	case http.MethodPost:
		var b ledger.Batch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"kind": "invalid_event", "message": "invalid JSON body: " + err.Error(),
			})
			return
		}
		// Writers take the read side of the maintenance gate: exclusive with
		// verify/recover but concurrent with each other (store mutex orders them).
		s.verifyMu.RLock()
		res, err := store.Append(&b)
		s.verifyMu.RUnlock()
		if err != nil {
			writeLedgerError(w, err)
			return
		}
		code := http.StatusCreated
		if !res.Accepted {
			code = http.StatusOK // identical retry: previously received
		}
		writeJSON(w, code, res)
	case http.MethodGet:
		// Convenience: /events?seq=N or /events?key=K; list otherwise.
		if q := r.URL.Query().Get("seq"); q != "" {
			n, err := strconv.ParseInt(q, 10, 64)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"message": "bad seq"})
				return
			}
			v, err := store.GetBySeq(n)
			if err != nil {
				writeLedgerError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, v)
			return
		}
		if k := r.URL.Query().Get("key"); k != "" {
			v, err := store.GetByKey(k)
			if err != nil {
				writeLedgerError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, v)
			return
		}
		st := store.ChainStatus()
		writeJSON(w, http.StatusOK, map[string]any{"tail_seq": st.TailSeq})
	default:
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleEventBy serves /events/seq/{n} and /events/key/{business-key}.
func (s *Server) handleEventBy(w http.ResponseWriter, r *http.Request, store *ledger.Store) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/events/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[1] == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "use /events/seq/N or /events/key/K"})
		return
	}
	switch parts[0] {
	case "seq":
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "bad seq"})
			return
		}
		v, err := store.GetBySeq(n)
		if err != nil {
			writeLedgerError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, v)
	case "key":
		v, err := store.GetByKey(parts[1])
		if err != nil {
			writeLedgerError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, v)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "unknown lookup " + parts[0]})
	}
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request, store *ledger.Store) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	from := int64(1)
	if q := r.URL.Query().Get("from"); q != "" {
		n, err := strconv.ParseInt(q, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": "bad from"})
			return
		}
		from = n
	}
	// Slow verification excludes appends for its duration. It re-checks the
	// on-disk prefix by reopening through the shared verify function.
	s.verifyMu.Lock()
	defer s.verifyMu.Unlock()
	res, err := store.Verify(from)
	if err != nil {
		writeLedgerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request, store *ledger.Store) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, store.ChainStatus())
}

// RecoverResult mirrors the store recovery report over HTTP.
type RecoverResult struct {
	Status   ledger.Status `json:"status"`
	Reopened bool          `json:"reopened"`
}

func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request, store *ledger.Store) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Recovery is exclusive: no appends while the prefix is being revalidated.
	s.verifyMu.Lock()
	defer s.verifyMu.Unlock()
	// Recovery already runs on open; this endpoint re-runs the shared scan by
	// reopening the directory and swapping the active store handle.
	dir := store.Dir()
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	if store != s.store {
		writeJSON(w, http.StatusConflict, map[string]string{"message": "recovery already in progress"})
		return
	}
	if err := store.Close(); err != nil {
		writeLedgerError(w, err)
		return
	}
	st, err := ledger.Open(dir)
	if err != nil {
		writeLedgerError(w, err)
		return
	}
	s.store = st
	writeJSON(w, http.StatusOK, RecoverResult{Status: st.ChainStatus(), Reopened: true})
}
