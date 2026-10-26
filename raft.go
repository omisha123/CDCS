package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// MetadataFSM is the Raft state machine. Every Raft node runs its own copy.
// Raft guarantees that Apply() is called with the exact same sequence of
// committed logs on every active cluster node, so the metadata store ends up
// identical everywhere without node-to-node HTTP calls of its own.
type MetadataFSM struct {
	mu    sync.RWMutex
	store map[string]FileMeta
}

func NewMetadataFSM() *MetadataFSM {
	return &MetadataFSM{store: make(map[string]FileMeta)}
}

// Apply is called by the Raft library itself, only after a command has been
// replicated to and acknowledged by a quorum of the active Raft cluster.
// This is the one and only place metadata is actually written to the FSM.
func (fsm *MetadataFSM) Apply(l *raft.Log) interface{} {
	var cmd LogCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("bad log entry: %w", err)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	switch cmd.Operation {
	case "SET_META":
		var meta FileMeta
		if err := json.Unmarshal(cmd.Payload, &meta); err != nil {
			return fmt.Errorf("bad SET_META payload: %w", err)
		}
		fsm.store[meta.ID] = meta

	case "DELETE_META":
		delete(fsm.store, string(cmd.Payload))

	default:
		return fmt.Errorf("unknown operation: %s", cmd.Operation)
	}

	return nil
}

// Get reads one entry straight from this node's local FSM state.
func (fsm *MetadataFSM) Get(fileID string) (FileMeta, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	m, ok := fsm.store[fileID]
	return m, ok
}

// All returns every entry currently held by this node's FSM.
func (fsm *MetadataFSM) All() []FileMeta {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	out := make([]FileMeta, 0, len(fsm.store))
	for _, m := range fsm.store {
		out = append(out, m)
	}

	return out
}

// fsmSnapshot is the minimal raft.FSMSnapshot implementation: it just
// JSON-encodes the map at the moment Snapshot() was called.
type fsmSnapshot struct {
	data map[string]FileMeta
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func (fsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	cp := make(map[string]FileMeta, len(fsm.store))
	for k, v := range fsm.store {
		cp[k] = v
	}

	return &fsmSnapshot{data: cp}, nil
}

func (fsm *MetadataFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var m map[string]FileMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	fsm.mu.Lock()
	fsm.store = m
	fsm.mu.Unlock()

	return nil
}

// SetupRaftNode initializes a Raft node.
//
// Active nodes are started with the same five-node peers map:
//
//	Raft-01 -> raft-01:60051
//	Raft-02 -> raft-02:60051
//	Raft-03 -> raft-03:60051
//	Raft-04 -> raft-04:60051
//	Raft-05 -> raft-05:60051
//
// Raft backup nodes are started without peers. They therefore do not become
// members of the initial five-node consensus cluster. They can be handled
// separately when a standby node needs to be promoted.
//
// bindAddr is what we actually listen on, for example 0.0.0.0:60051.
//
// advertiseAddr is what we tell other Raft peers to dial us on. In Docker
// this is the service name and port, for example raft-01:60051.
func SetupRaftNode(nodeID, bindAddr, advertiseAddr string, peers map[string]string) (*raft.Raft, *MetadataFSM, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	config.HeartbeatTimeout = 250 * time.Millisecond
	config.ElectionTimeout = 600 * time.Millisecond
	config.LeaderLeaseTimeout = 200 * time.Millisecond

	if advertiseAddr == "" {
		advertiseAddr = bindAddr
	}

	addr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve advertise address for %s: %w", nodeID, err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create TCP transport: %w", err)
	}

	// Storage is always durable: log + term/vote in BoltDB, snapshots as
	// files. RAFT_DATA_DIR picks the folder; if unset it defaults to
	// raft-data/<nodeID> in the working directory. A restarted node rejoins
	// with its log and current term. To wipe a cluster on purpose, delete
	// the data directories.
	dir := os.Getenv("RAFT_DATA_DIR")
	if dir == "" {
		dir = filepath.Join("raft-data", nodeID)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("failed to create raft data dir %s: %w", dir, err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open raft BoltDB store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dir, 2, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create raft snapshot store: %w", err)
	}

	var (
		logStore    raft.LogStore    = boltStore
		stableStore raft.StableStore = boltStore
	)

	fsm := NewMetadataFSM()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	// Active Raft nodes receive the complete five-node configuration.
	//
	// Backup Raft nodes receive an empty peers map and therefore do not
	// bootstrap themselves into the active five-node consensus group.
	if len(peers) > 0 {
		servers := make([]raft.Server, 0, len(peers))

		for id, addr := range peers {
			peerAddr := addr

			if id == nodeID {
				peerAddr = string(transport.LocalAddr())
			}

			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(peerAddr),
			})
		}

		configuration := raft.Configuration{
			Servers: servers,
		}

		if err := r.BootstrapCluster(configuration).Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, nil, fmt.Errorf("failed to bootstrap raft cluster: %w", err)
		}
	} else if nodeID == "Raft-01" {
		// Fallback for a single-node development run with no peers configured.
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		if err := r.BootstrapCluster(configuration).Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, nil, fmt.Errorf("failed to bootstrap single-node raft cluster: %w", err)
		}
	}

	return r, fsm, nil
}