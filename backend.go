package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	pb "cdcs1/cdcspb"
)

const rsTotalShards = 6

// ClusterConfig represents the physical IP mappings read from cluster.json
type ClusterConfig struct {
	StorageNodes    map[string]string `json:"storage_nodes"`
	RaftNodes       map[string]string `json:"raft_nodes"`
	BackupNodes     []string          `json:"backup_nodes"`
	BackupNodeAddrs map[string]string `json:"backup_node_addrs"`
	RaftBackupNodes []string          `json:"raft_backup_nodes"`
	// RaftBackupAddrs maps each standby raft node's ID to its raft address
	// (e.g. "Raft-06": "172.18.37.153:60051"). Needed so the orchestrator can
	// promote a standby without guessing its address.
	RaftBackupAddrs map[string]string `json:"raft_backup_addrs"`
}

type ShardMapping struct {
	ShardID string `json:"shard_id"`
	NodeID  string `json:"node_id"`
}

type FileMeta struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Size         int64             `json:"size"`
	Mappings     []ShardMapping    `json:"mappings"`
	ChunkMap     map[string]string `json:"chunk_map"`
	BackupMap    map[string]string `json:"backup_map"`
	HomeMap      map[string]string `json:"home_map"`
	DataShards   int               `json:"data_shards"`
	ParityShards int               `json:"parity_shards"`
	ShardSize    int               `json:"shard_size"`
	ChecksumMap  map[string]string `json:"checksum_map"`
	LastAccessed int64             `json:"last_accessed"`
	IsPinned     bool              `json:"is_pinned"`
	UploadedAt   int64             `json:"uploaded_at"`
}

type uploadWALEntry struct {
	FileID    string `json:"file_id"`
	StartedAt int64  `json:"started_at"`
}

type NodeStatus struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	Status   string `json:"status"`
	IsBackup bool   `json:"is_backup"`
}

// Backend acts strictly as the Orchestrator/API Gateway.
// It has ZERO local memory dependencies on the 15 physical PCs.
type Backend struct {
	config    ClusterConfig
	fileMeta  map[string]FileMeta
	db        *pebble.DB
	cc        *ClusterClient
	mu        sync.RWMutex
	sfGroup   *SingleFlightGroup
	sfTotal   int64
	sfDeduped int64
	sfViews   int64
	events    []LogEvent
	eventsMu  sync.Mutex
	// Used for chaos testing to simulate network partitions without shutting down physical PCs
	killedNodes map[string]bool
	// Upload IDs reserved by an upload that is still running, so two
	// concurrent uploads of the same ID cannot both pass the "already in use"
	// check. Guarded by mu.
	uploadsInFlight map[string]bool
	// Simulated raft partitions (chaos testing): the orchestrator treats these
	// raft nodes as unreachable even though the processes keep running.
	killedRaft map[string]bool

	// Raft standby auto-promotion bookkeeping.
	raftDeadStreak map[string]int  // main raft node ID -> consecutive dead health-check count
	raftPromoted   map[string]bool // main raft node ID -> already had a standby promoted in its place
	raftLeaderAPI  string          // last raft API address that accepted a write (leader cache)
	usedBackups    map[string]bool // standby raft node ID -> already promoted (don't reuse)

	// Storage-node auto-failover bookkeeping (mirrors the raft one above).
	storageDeadStreak map[string]int // storage node ID -> consecutive unreachable health-check count

	// Counters/histograms surfaced by HandleMetrics (metrics.go).
	sfInflight             int64
	shardFetchSuccessTotal int64
	shardFetchFailTotal    int64
	reconstructTotal       int64
	uploadRequestsTotal    int64
	uploadSuccessTotal     int64
	downloadSuccessTotal   int64
	killEventsTotal        int64
	reviveEventsTotal      int64
	sweeperEvictionsTotal  int64
	downloadLatencyMs      *Histogram
	uploadLatencyMs        *Histogram
}

type LogEvent struct {
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

var fileIDSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// maxUploadSize is the hard cap on one uploaded file. The whole file is held
// in RAM (plus its Reed-Solomon shards, ~1.5x more) while it is encoded, so
// this is what bounds orchestrator memory per upload. Raise it only if the
// orchestrator machine has the RAM for it.
const maxUploadSize = 100 << 20

// sameOrigin rejects cross-site requests to state-changing endpoints: a
// browser always sends Origin on cross-site POST/DELETE, and it must match
// the host the request was sent to. Requests without Origin (curl, scripts)
// are allowed.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

// envIntOrDefault reads a positive integer from the named environment
// variable, falling back to def if the variable is unset, empty, or not a
// valid positive integer. Shared by the raft and storage failover watchers
// so their streak thresholds are tunable without a code change.
func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		fmt.Printf("[Config] WARNING: invalid %s=%q, falling back to %d\n", key, v, def)
	}
	return def
}

func sanitizeFileID(id string) string {
	id = strings.ReplaceAll(id, "..", "")
	id = fileIDSanitizer.ReplaceAllString(id, "_")
	if id == "" {
		id = fmt.Sprintf("file_%d", time.Now().UnixNano())
	}
	return id
}

func shardChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func verifyChecksum(data []byte, want string) bool {
	if want == "" {
		return true
	}
	return shardChecksum(data) == want
}

func shardIndexOf(shardID string) uint32 {
	n, _ := strconv.Atoi(strings.TrimPrefix(shardID, "shard_"))
	if n <= 0 {
		return 0
	}
	return uint32(n - 1)
}

// NewBackend reads the cluster.json file and initializes the orchestrator
func NewBackend(configPath string) *Backend {
	var cfg ClusterConfig
	data, err := os.ReadFile(configPath)
	if err == nil {
		json.Unmarshal(data, &cfg)
	} else {
		// Fallback for immediate testing if the file is missing
		cfg = ClusterConfig{
			StorageNodes: map[string]string{
				"Node-01": "192.168.1.201:50051",
				"Node-02": "192.168.1.202:50051",
				"Node-03": "192.168.1.203:50051",
				"Node-04": "192.168.1.204:50051",
				"Node-05": "192.168.1.205:50051",
				"Node-06": "192.168.1.206:50051",
			},
			RaftNodes: map[string]string{
				"Raft-01": "192.168.1.101:60051",
				"Raft-02": "192.168.1.102:60051",
				"Raft-03": "192.168.1.103:60051",
				"Raft-04": "192.168.1.104:60051",
				"Raft-05": "192.168.1.105:60051",
			},
			BackupNodes: []string{"Node-07", "Node-08"},
			BackupNodeAddrs: map[string]string{
				"Node-07": "192.168.1.207:50051",
				"Node-08": "192.168.1.208:50051",
			},
			RaftBackupNodes: []string{"Raft-06", "Raft-07"},
			RaftBackupAddrs: map[string]string{
				"Raft-06": "192.168.1.106:60051",
				"Raft-07": "192.168.1.107:60051",
			},
		}
	}

	// METADATA_DB_PATH lets Docker put the database on a mounted volume so
	// file metadata survives container restarts.
	dbPath := os.Getenv("METADATA_DB_PATH")
	if dbPath == "" {
		dbPath = "metadata.db"
	}

	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		panic(fmt.Sprintf("Failed to open orchestrator metadata Pebble DB: %v", err))
	}

	allStorageNodes := make(map[string]string, len(cfg.StorageNodes)+len(cfg.BackupNodeAddrs))
	for nodeID, addr := range cfg.StorageNodes {
		allStorageNodes[nodeID] = addr
	}
	for nodeID, addr := range cfg.BackupNodeAddrs {
		allStorageNodes[nodeID] = addr
	}

	b := &Backend{
		config:            cfg,
		fileMeta:          make(map[string]FileMeta),
		db:                db,
		cc:                NewClusterClient(allStorageNodes),
		sfGroup:           NewSingleFlightGroup(),
		killedNodes:       make(map[string]bool),
		uploadsInFlight:   make(map[string]bool),
		killedRaft:        make(map[string]bool),
		raftDeadStreak:    make(map[string]int),
		raftPromoted:      make(map[string]bool),
		usedBackups:       make(map[string]bool),
		storageDeadStreak: make(map[string]int),
		downloadLatencyMs: NewHistogram([]float64{
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
		}),
		uploadLatencyMs: NewHistogram([]float64{
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
		}),
	}

	b.loadMetadataFromDB()
	b.loadRaftTopology()
	b.recoverIncompleteUploads()
	b.startUnifiedSweeper()
	b.startRaftFailoverWatcher()
	b.startStorageFailoverWatcher()

	return b
}

func (b *Backend) loadMetadataFromDB() {
	b.mu.Lock()
	defer b.mu.Unlock()

	iter, err := b.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("file:"),
		UpperBound: []byte("file;"),
	})
	if err != nil {
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var meta FileMeta
		if err := json.Unmarshal(iter.Value(), &meta); err == nil {
			b.fileMeta[meta.ID] = meta
		}
	}
}

func (b *Backend) walKey(fileID string) []byte {
	return fmt.Appendf(nil, "wal:%s", fileID)
}

func (b *Backend) beginUploadWAL(fileID string) {
	entry := uploadWALEntry{
		FileID:    fileID,
		StartedAt: time.Now().Unix(),
	}

	if data, err := json.Marshal(entry); err == nil {
		_ = b.db.Set(b.walKey(fileID), data, pebble.Sync)
	}
}

func (b *Backend) commitUploadWAL(fileID string) {
	_ = b.db.Delete(b.walKey(fileID), pebble.Sync)
}

// saveMetaLocal writes meta to the orchestrator's Pebble DB and in-memory map only.
func (b *Backend) saveMetaLocal(fileID string, meta FileMeta) {
	metaBytes, _ := json.Marshal(meta)
	_ = b.db.Set([]byte("file:"+fileID), metaBytes, pebble.Sync)

	b.mu.Lock()
	b.fileMeta[fileID] = meta
	b.mu.Unlock()
}

// saveMeta is LOCAL-FIRST: it updates the orchestrator's own copy, then
// commits to Raft and returns the Raft error. Use it for system-initiated
// updates (reroute, restore, last-accessed) where the local view must move
// even if Raft is temporarily unavailable. Callers must check the error.
func (b *Backend) saveMeta(fileID string, meta FileMeta) error {
	b.saveMetaLocal(fileID, meta)

	metaBytes, _ := json.Marshal(meta)
	cmd := LogCommand{Operation: "SET_META", Payload: metaBytes}

	if err := b.raftApply(cmd); err != nil {
		fmt.Printf("[Raft] SET_META failed for %s: %v\n", fileID, err)
		return fmt.Errorf("raft commit failed: %w", err)
	}
	return nil
}

// saveMetaRaftFirst is RAFT-FIRST: the local copy is only written after the
// Raft commit succeeds, so a failure leaves nothing behind locally. Use it
// for user-initiated changes (upload, pin) so a failed commit is a clean
// failure instead of a local copy that is ahead of Raft.
func (b *Backend) saveMetaRaftFirst(fileID string, meta FileMeta) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	cmd := LogCommand{Operation: "SET_META", Payload: metaBytes}
	if err := b.raftApply(cmd); err != nil {
		fmt.Printf("[Raft] SET_META failed for %s: %v\n", fileID, err)
		return fmt.Errorf("raft commit failed: %w", err)
	}

	b.saveMetaLocal(fileID, meta)
	return nil
}

func (b *Backend) raftApply(cmd LogCommand) error {
	b.mu.RLock()
	cachedLeader := b.raftLeaderAPI
	live := make([]string, 0, len(b.config.RaftNodes))
	dead := make([]string, 0)
	for id, addr := range b.config.RaftNodes {
		if b.killedRaft[id] {
			continue // simulated partition: don't even try it
		}
		apiAddr := deriveAPIAddr(addr)
		// Nodes the watcher currently counts as dead go last, not first.
		if b.raftDeadStreak[id] > 0 {
			dead = append(dead, apiAddr)
		} else {
			live = append(live, apiAddr)
		}
	}
	b.mu.RUnlock()

	// Order: cached leader, then healthy nodes, then dead ones as a last resort.
	candidates := make([]string, 0, len(live)+len(dead)+1)
	if cachedLeader != "" {
		candidates = append(candidates, cachedLeader)
	}
	for _, a := range append(live, dead...) {
		if a != cachedLeader {
			candidates = append(candidates, a)
		}
	}

	if len(candidates) == 0 {
		return fmt.Errorf("no Raft nodes configured")
	}

	var lastErr error
	for _, apiAddr := range candidates {
		err := SubmitLogCommand(apiAddr, cmd)
		if err == nil {
			b.mu.Lock()
			b.raftLeaderAPI = apiAddr
			b.mu.Unlock()
			return nil
		}
		lastErr = err

		// The cached address failed: forget it so we don't retry it first.
		if apiAddr == cachedLeader {
			b.mu.Lock()
			if b.raftLeaderAPI == cachedLeader {
				b.raftLeaderAPI = ""
			}
			b.mu.Unlock()
		}
	}

	return lastErr
}

const raftTopologyKey = "raft_topology"

// raftTopology is the part of the orchestrator's raft view that failover
// changes at runtime. It is persisted so an orchestrator restart doesn't
// reload cluster.json and forget that a standby replaced a dead node.
type raftTopology struct {
	RaftNodes   map[string]string `json:"raft_nodes"`
	Promoted    map[string]bool   `json:"promoted"`
	UsedBackups map[string]bool   `json:"used_backups"`
}

// persistRaftTopologyLocked saves the current raft topology. The caller
// must already hold b.mu.
func (b *Backend) persistRaftTopologyLocked() {
	data, err := json.Marshal(raftTopology{
		RaftNodes:   b.config.RaftNodes,
		Promoted:    b.raftPromoted,
		UsedBackups: b.usedBackups,
	})
	if err != nil {
		fmt.Printf("[Raft] could not marshal topology: %v\n", err)
		return
	}
	if err := b.db.Set([]byte(raftTopologyKey), data, pebble.Sync); err != nil {
		fmt.Printf("[Raft] could not persist topology: %v\n", err)
	}
}

// loadRaftTopology overlays a previously saved topology on top of
// cluster.json. Set RESET_RAFT_TOPOLOGY=1 to discard it (for example after
// editing raft addresses in cluster.json on purpose).
func (b *Backend) loadRaftTopology() {
	key := []byte(raftTopologyKey)

	if os.Getenv("RESET_RAFT_TOPOLOGY") == "1" {
		_ = b.db.Delete(key, pebble.Sync)
		return
	}

	val, closer, err := b.db.Get(key)
	if err != nil {
		return // nothing saved yet
	}
	defer closer.Close()

	var t raftTopology
	if err := json.Unmarshal(val, &t); err != nil || len(t.RaftNodes) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.config.RaftNodes = t.RaftNodes
	if t.Promoted != nil {
		b.raftPromoted = t.Promoted
	}
	if t.UsedBackups != nil {
		b.usedBackups = t.UsedBackups
	}
	fmt.Printf("[Raft] restored saved topology (%d voters)\n", len(t.RaftNodes))
}

// isNodeKilled reports whether a storage node is under a simulated partition.
// killedNodes is written by HandleKillNode, so reads must be locked.
func (b *Backend) isNodeKilled(nodeID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.killedNodes[nodeID]
}

func (b *Backend) recoverIncompleteUploads() {
	iter, err := b.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("wal:"),
		UpperBound: []byte("wal;"),
	})
	if err != nil {
		return
	}

	var pending []string

	for iter.First(); iter.Valid(); iter.Next() {
		var entry uploadWALEntry
		if err := json.Unmarshal(iter.Value(), &entry); err == nil {
			pending = append(pending, entry.FileID)
		}
	}

	iter.Close()

	ctx := context.Background()

	for _, fileID := range pending {
		if _, committed := b.fileMeta[fileID]; committed {
			b.commitUploadWAL(fileID)
			continue
		}

		for i := 0; i < rsTotalShards; i++ {
			for nodeID := range b.config.StorageNodes {
				_ = b.cc.DeleteShard(ctx, nodeID, fileID, uint32(i))
			}

			for _, nodeID := range b.config.BackupNodes {
				_ = b.cc.DeleteShard(ctx, nodeID, fileID, uint32(i))
			}
		}

		b.commitUploadWAL(fileID)
	}
}

// HandleNodes reports every physical storage node's online/offline network status to the frontend.
func (b *Backend) HandleNodes(w http.ResponseWriter, r *http.Request) {
	// Snapshot shared state under a short lock; the pings below are network
	// calls (up to pingTimeout each) and must not run while holding b.mu.
	b.mu.RLock()

	allNodes := make(map[string]string, len(b.config.StorageNodes)+len(b.config.BackupNodeAddrs))

	for id, addr := range b.config.StorageNodes {
		allNodes[id] = addr
	}

	for id, addr := range b.config.BackupNodeAddrs {
		allNodes[id] = addr
	}

	backupSet := make(map[string]bool, len(b.config.BackupNodes))
	for _, id := range b.config.BackupNodes {
		backupSet[id] = true
	}

	killed := make(map[string]bool, len(b.killedNodes))
	for id, v := range b.killedNodes {
		killed[id] = v
	}

	b.mu.RUnlock()

	statuses := make([]NodeStatus, 0, len(allNodes))

	var wg sync.WaitGroup
	var smu sync.Mutex

	for id, addr := range allNodes {
		wg.Add(1)

		go func(id, addr string) {
			defer wg.Done()

			status := "ONLINE"

			// If chaos test simulated a kill OR the physical gRPC connection is actually dead
			if killed[id] || !b.cc.PingNode(id) {
				status = "OFFLINE"
			}

			smu.Lock()
			statuses = append(statuses, NodeStatus{
				ID:       id,
				Addr:     addr,
				Status:   status,
				IsBackup: backupSet[id],
			})
			smu.Unlock()
		}(id, addr)
	}

	wg.Wait()

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].ID < statuses[j].ID
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// fetchRaftStatuses polls every main raft node's /status over HTTP and returns
// a map of nodeID -> parsed status (or a synthetic "Dead" entry if unreachable).
// Shared by HandleRaftStatus (for the dashboard) and the auto-promote watcher.
func (b *Backend) fetchRaftStatuses() map[string]map[string]interface{} {
	b.mu.RLock()
	raftNodes := make(map[string]string, len(b.config.RaftNodes))
	killed := make(map[string]bool, len(b.killedRaft))
	for id, addr := range b.config.RaftNodes {
		raftNodes[id] = addr
		if b.killedRaft[id] {
			killed[id] = true
		}
	}
	b.mu.RUnlock()

	results := make(map[string]map[string]interface{}, len(raftNodes))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for id, addr := range raftNodes {
		wg.Add(1)
		go func(nodeID, raftAddr string) {
			defer wg.Done()

			statusEntry := map[string]interface{}{
				"id":           nodeID,
				"address":      raftAddr,
				"state":        "Dead",
				"commit_index": uint64(0),
				"term":         uint64(0),
			}

			// Simulated partition: report Dead without contacting the node.
			if killed[nodeID] {
				mu.Lock()
				results[nodeID] = statusEntry
				mu.Unlock()
				return
			}

			apiAddr := deriveAPIAddr(raftAddr)
			client := http.Client{Timeout: 2 * time.Second}

			resp, err := client.Get("http://" + apiAddr + "/status")
			if err == nil {
				defer resp.Body.Close()

				var apiRes map[string]interface{}
				if decodeErr := json.NewDecoder(resp.Body).Decode(&apiRes); decodeErr == nil {
					if state, ok := apiRes["state"].(string); ok {
						statusEntry["state"] = state
					}
					if cidx, ok := apiRes["commit_index"]; ok {
						statusEntry["commit_index"] = cidx
					}
					if term, ok := apiRes["term"]; ok {
						statusEntry["term"] = term
					}
				}
			}

			mu.Lock()
			results[nodeID] = statusEntry
			mu.Unlock()
		}(id, addr)
	}

	wg.Wait()

	// Any call (dashboard, watcher, /metrics) can be the first to see the
	// new leader after a leader kill.
	ObserveLeaderElectionIfPending(results)

	return results
}

func (b *Backend) HandleRaftStatus(w http.ResponseWriter, r *http.Request) {
	live := b.fetchRaftStatuses()

	var statuses []map[string]interface{}
	for _, entry := range live {
		statuses = append(statuses, entry)
	}

	b.mu.RLock()
	raftBackupNodes := append([]string(nil), b.config.RaftBackupNodes...)
	raftBackupAddrs := make(map[string]string, len(b.config.RaftBackupAddrs))
	for k, v := range b.config.RaftBackupAddrs {
		raftBackupAddrs[k] = v
	}
	usedBackups := make(map[string]bool, len(b.usedBackups))
	for k, v := range b.usedBackups {
		usedBackups[k] = v
	}
	b.mu.RUnlock()

	// Actually check each standby's own /status instead of assuming - a
	// standby whose container crashed (e.g. missing RAFT_SEED) should show
	// as unreachable, not silently look "online" just because it's listed
	// in config.
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, nodeID := range raftBackupNodes {
		// A promoted standby now lives in config.RaftNodes and is already
		// in `live` with its real Raft state; don't list it twice.
		if _, inLive := live[nodeID]; inLive {
			continue
		}

		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			addr := raftBackupAddrs[id]
			reachable := false
			if addr != "" {
				apiAddr := deriveAPIAddr(addr)
				client := http.Client{Timeout: 2 * time.Second}
				if resp, err := client.Get("http://" + apiAddr + "/status"); err == nil {
					resp.Body.Close()
					reachable = true
				}
			}

			state := "Standby (not running)"
			if reachable {
				state = "Standby"
			}
			if usedBackups[id] {
				state = "Promoted"
				if !reachable {
					state = "Promoted (unreachable)"
				}
			}

			mu.Lock()
			statuses = append(statuses, map[string]interface{}{
				"id":           id,
				"address":      addr,
				"state":        state,
				"reachable":    reachable,
				"commit_index": uint64(0),
			})
			mu.Unlock()
		}(nodeID)
	}
	wg.Wait()

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i]["id"].(string) < statuses[j]["id"].(string)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// HandlePromoteRaft manually promotes a standby raft node (e.g. ?id=Raft-06)
// to a full voter. Looks the standby's address up in RaftBackupAddrs, finds
// any reachable raft node's API (it'll redirect to the leader if needed), and
// asks it to run /promote.
func (b *Backend) HandlePromoteRaft(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		http.Error(w, "missing ?id=", http.StatusBadRequest)
		return
	}

	b.mu.RLock()
	addr, ok := b.config.RaftBackupAddrs[nodeID]
	raftNodes := make([]string, 0, len(b.config.RaftNodes))
	for _, a := range b.config.RaftNodes {
		raftNodes = append(raftNodes, a)
	}
	b.mu.RUnlock()

	if !ok {
		http.Error(w, fmt.Sprintf("unknown standby node %s (not in raft_backup_addrs)", nodeID), http.StatusNotFound)
		return
	}

	var lastErr error
	for _, raftAddr := range raftNodes {
		apiAddr := deriveAPIAddr(raftAddr)
		if err := PromoteToVoter(apiAddr, nodeID, addr); err == nil {
			b.mu.Lock()
			b.usedBackups[nodeID] = true
			b.config.RaftNodes[nodeID] = addr
			b.persistRaftTopologyLocked()
			b.mu.Unlock()
			b.logEvent(fmt.Sprintf("Promoted standby %s to full raft voter", nodeID))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%s promoted to voter", nodeID)))
			return
		} else {
			lastErr = err
		}
	}

	http.Error(w, fmt.Sprintf("promote failed: %v", lastErr), http.StatusInternalServerError)
}

// HandleKillRaft simulates a network partition for one raft node, the raft
// counterpart of /kill-node. The process keeps running, but the orchestrator
// stops contacting it and reports it Dead, so the failover watcher promotes
// a standby after RAFT_FAILOVER_STREAK checks. (To test a real crash, stop
// the container instead, e.g. `docker-compose stop raft-03`.)
func (b *Backend) HandleKillRaft(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		http.Error(w, "missing ?id=", http.StatusBadRequest)
		return
	}

	// Find out whether this is the current leader before the partition is
	// simulated, so the election that follows can be timed.
	wasLeader := false
	if st, found := b.fetchRaftStatuses()[nodeID]; found && st["state"] == "Leader" {
		wasLeader = true
	}

	b.mu.Lock()
	addr, ok := b.config.RaftNodes[nodeID]
	if !ok {
		b.mu.Unlock()
		http.Error(w, fmt.Sprintf("unknown raft node %s", nodeID), http.StatusNotFound)
		return
	}
	alreadyKilled := b.killedRaft[nodeID]
	b.killedRaft[nodeID] = true
	if b.raftLeaderAPI == deriveAPIAddr(addr) {
		b.raftLeaderAPI = "" // don't keep routing writes to the "killed" leader
	}
	b.mu.Unlock()

	if alreadyKilled {
		w.WriteHeader(http.StatusOK)
		return
	}
	atomic.AddInt64(&b.killEventsTotal, 1)
	if wasLeader {
		MarkLeaderKilled()
	}
	b.logEvent(fmt.Sprintf("Chaos: simulated partition of raft node %s", nodeID))
	w.WriteHeader(http.StatusOK)
}

// HandleReviveRaft ends a simulated partition. If failover already replaced
// the node, it is no longer in the raft config and this only clears the flag.
func (b *Backend) HandleReviveRaft(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		http.Error(w, "missing ?id=", http.StatusBadRequest)
		return
	}

	b.mu.Lock()
	wasKilled := b.killedRaft[nodeID]
	delete(b.killedRaft, nodeID)
	delete(b.raftDeadStreak, nodeID)
	b.mu.Unlock()

	if wasKilled {
		atomic.AddInt64(&b.reviveEventsTotal, 1)
	}
	b.logEvent(fmt.Sprintf("Chaos: restored raft node %s", nodeID))
	w.WriteHeader(http.StatusOK)
}

// startRaftFailoverWatcher polls raft node health every 5s. If a main raft
// node has been unreachable for raftFailoverStreak consecutive checks
// (raftFailoverStreak * 5s, 15s by default), it's considered dead and the
// next unused standby (in RaftBackupNodes order) is automatically promoted
// to take its place - mirroring the Reed-Solomon backup-node failover
// already used for storage, but for the raft voting set. The streak
// threshold is tunable via RAFT_FAILOVER_STREAK.
func (b *Backend) startRaftFailoverWatcher() {
	streakThreshold := envIntOrDefault("RAFT_FAILOVER_STREAK", 3)
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		// Give the cluster time to actually finish starting up before judging
		// anyone "dead" - without this, main raft nodes that are still
		// binding their ports during the first few seconds of a
		// `docker-compose up` get read as unreachable 3 times in a row
		// (15s) and trigger a real, wrong promotion before anything has
		// actually failed. This runs in the background goroutine, not
		// before it, so it doesn't delay the orchestrator's own startup.
		time.Sleep(30 * time.Second)

		for range ticker.C {
			live := b.fetchRaftStatuses()

			b.mu.Lock()
			for nodeID, entry := range live {
				if entry["state"] == "Dead" {
					b.raftDeadStreak[nodeID]++
				} else {
					b.raftDeadStreak[nodeID] = 0
				}
			}

			var deadNode string
			for nodeID, streak := range b.raftDeadStreak {
				if streak >= streakThreshold && !b.raftPromoted[nodeID] {
					deadNode = nodeID
					break
				}
			}

			var standbyID, standbyAddr string
			if deadNode != "" {
				for _, id := range b.config.RaftBackupNodes {
					if !b.usedBackups[id] {
						standbyID = id
						standbyAddr = b.config.RaftBackupAddrs[id]
						break
					}
				}
			}

			// Contact healthy nodes only, so a dead one can't burn a full HTTP
			// timeout per attempt. Fall back to everyone if none look healthy.
			deadAddr := b.config.RaftNodes[deadNode]
			raftNodes := make([]string, 0, len(b.config.RaftNodes))
			allNodes := make([]string, 0, len(b.config.RaftNodes))
			for id, a := range b.config.RaftNodes {
				allNodes = append(allNodes, a)
				if b.raftDeadStreak[id] == 0 && !b.killedRaft[id] {
					raftNodes = append(raftNodes, a)
				}
			}
			if len(raftNodes) == 0 {
				raftNodes = allNodes
			}
			b.mu.Unlock()

			if deadNode == "" || standbyID == "" {
				continue
			}

			promoted := false
			var lastErr error
			for _, raftAddr := range raftNodes {
				apiAddr := deriveAPIAddr(raftAddr)
				if err := PromoteToVoter(apiAddr, standbyID, standbyAddr); err == nil {
					promoted = true
					break
				} else {
					lastErr = err
				}
			}

			// Promotion succeeded: now remove the dead voter from the Raft
			// configuration so the voter count doesn't keep growing, and
			// swap it for the standby in the orchestrator's own node list.
			// (Promote first, remove second: the cluster never drops below
			// the voters it had before the failure.)
			removeErr := error(nil)
			if promoted {
				removeErr = fmt.Errorf("no reachable raft node to remove through")
				for _, raftAddr := range raftNodes {
					if raftAddr == deadAddr {
						continue // don't wait on the node we know is dead
					}
					if err := RemoveFromCluster(deriveAPIAddr(raftAddr), deadNode); err == nil {
						removeErr = nil
						break
					} else {
						removeErr = err
					}
				}
			}

			b.mu.Lock()
			if promoted {
				b.raftPromoted[deadNode] = true
				b.usedBackups[standbyID] = true
				b.config.RaftNodes[standbyID] = standbyAddr
				delete(b.config.RaftNodes, deadNode)
				delete(b.raftDeadStreak, deadNode)
				delete(b.killedRaft, deadNode)
				b.raftLeaderAPI = "" // topology changed; re-discover the leader
				b.persistRaftTopologyLocked()
				b.mu.Unlock()
				if removeErr != nil {
					fmt.Printf("[RaftWatcher] promoted %s but could not remove dead %s from raft config: %v\n", standbyID, deadNode, removeErr)
					b.logEvent(fmt.Sprintf("WARNING: %s promoted but dead node %s is still in the raft configuration (remove failed)", standbyID, deadNode))
				}
				msg := fmt.Sprintf("Auto-failover: %s unreachable for %ds, promoted standby %s in its place", deadNode, streakThreshold*5, standbyID)
				fmt.Println("[RaftWatcher] " + msg)
				b.logEvent(msg)
			} else {
				b.mu.Unlock()
				fmt.Printf("[RaftWatcher] tried to promote %s for dead %s but failed: %v\n", standbyID, deadNode, lastErr)
			}
		}
	}()
}

// ----------------------------------------------------------------------
// FULLY NETWORKED KILL NODE (Simulated Chaos Engineering)
// ----------------------------------------------------------------------

func (b *Backend) HandleKillNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")

	b.mu.Lock()
	_, isMain := b.config.StorageNodes[nodeID]
	_, isBackup := b.config.BackupNodeAddrs[nodeID]
	if nodeID == "" || (!isMain && !isBackup) {
		b.mu.Unlock()
		http.Error(w, fmt.Sprintf("unknown storage node %q", nodeID), http.StatusNotFound)
		return
	}
	if b.killedNodes[nodeID] {
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK) // already killed: idempotent, not a new event
		return
	}
	b.killedNodes[nodeID] = true
	b.mu.Unlock()
	atomic.AddInt64(&b.killEventsTotal, 1)

	fmt.Printf("=> Simulating Network Partition for %s\n", nodeID)
	b.rerouteShardsAwayFrom(nodeID)

	w.WriteHeader(http.StatusOK)
}

// rerouteShardsAwayFrom moves every shard currently pointing at nodeID over
// to its already-recorded backup. Called both by the manual /kill-node
// chaos-test endpoint AND by startStorageFailoverWatcher when a node is
// automatically detected as unreachable - previously this only ran when you
// explicitly hit /kill-node, so a real container/PC going down outside that
// endpoint never got rerouted at all.
func (b *Backend) rerouteShardsAwayFrom(nodeID string) {
	b.mu.RLock()

	filesToUpdate := make(map[string]FileMeta)

	for fileID, meta := range b.fileMeta {
		needsUpdate := false

		newChunkMap := make(map[string]string)
		for k, v := range meta.ChunkMap {
			newChunkMap[k] = v
		}

		newMappings := make([]ShardMapping, len(meta.Mappings))
		copy(newMappings, meta.Mappings)

		for i, mapping := range newMappings {
			chunkID := fmt.Sprintf("%s_%s", fileID, mapping.ShardID)

			if newChunkMap[chunkID] == nodeID {
				if meta.BackupMap[chunkID] == "" || meta.BackupMap[chunkID] == nodeID {
					// No verified backup copy exists; leave the mapping alone
					// instead of pointing it at a shard that isn't there.
					continue
				}
				newChunkMap[chunkID] = meta.BackupMap[chunkID]
				newMappings[i].NodeID = meta.BackupMap[chunkID]
				needsUpdate = true
			}
		}

		if needsUpdate {
			meta.ChunkMap = newChunkMap
			meta.Mappings = newMappings
			filesToUpdate[fileID] = meta
		}
	}

	b.mu.RUnlock()

	for fileID, meta := range filesToUpdate {
		if err := b.saveMeta(fileID, meta); err != nil {
			b.logEvent(fmt.Sprintf("WARNING: reroute of %s applied locally but Raft commit failed: %v", fileID, err))
		}
		fmt.Printf("=> RS Failover: Rerouted shards for file %s away from %s\n", fileID, nodeID)
	}
}

// ----------------------------------------------------------------------
// FULLY NETWORKED REVIVE NODE (Network Restoration)
// ----------------------------------------------------------------------

func (b *Backend) HandleReviveNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")

	b.mu.Lock()
	wasKilled := b.killedNodes[nodeID]
	delete(b.killedNodes, nodeID)
	b.mu.Unlock()
	if wasKilled {
		atomic.AddInt64(&b.reviveEventsTotal, 1)
	}

	fmt.Printf("=> Restoring Network Path to %s\n", nodeID)
	b.restoreShardsHomeTo(nodeID)

	w.WriteHeader(http.StatusOK)
}

// restoreShardsHomeTo pulls shards back onto nodeID from wherever they got
// rerouted to, once nodeID is confirmed reachable again. Shared by the
// manual /revive-node endpoint and the automatic storage watcher below.
func (b *Backend) restoreShardsHomeTo(nodeID string) {
	healStart := time.Now()
	ctx := context.Background()

	type restoreTask struct {
		fileID     string
		chunkID    string
		shardIdx   uint32
		backupNode string
	}

	// Phase 1: snapshot what needs moving under a short read lock. The
	// network copies below must NOT run while holding b.mu, or one slow node
	// would block every writer (uploads, pins, deletes) for the duration.
	var tasks []restoreTask

	b.mu.RLock()
	for fileID, meta := range b.fileMeta {
		for _, mapping := range meta.Mappings {
			chunkID := fmt.Sprintf("%s_%s", fileID, mapping.ShardID)

			if meta.HomeMap[chunkID] == nodeID && meta.ChunkMap[chunkID] != nodeID {
				tasks = append(tasks, restoreTask{
					fileID:     fileID,
					chunkID:    chunkID,
					shardIdx:   shardIndexOf(mapping.ShardID),
					backupNode: meta.ChunkMap[chunkID],
				})
			}
		}
	}
	b.mu.RUnlock()

	// Phase 2: copy shards back home without holding any lock.
	moved := make(map[string][]restoreTask)

	for _, t := range tasks {
		data, checksum, err := b.cc.GetShard(ctx, t.backupNode, t.fileID, t.shardIdx)
		if err != nil {
			fmt.Printf("=> RS Restoration: could not read %s shard %d from %s to restore to %s: %v\n", t.fileID, t.shardIdx, t.backupNode, nodeID, err)
			continue
		}

		if storeErr := b.cc.PutShard(ctx, nodeID, t.fileID, t.shardIdx, data, checksum); storeErr != nil {
			fmt.Printf("=> RS Restoration: failed writing %s shard %d back to %s: %v\n", t.fileID, t.shardIdx, nodeID, storeErr)
			continue
		}

		moved[t.fileID] = append(moved[t.fileID], t)
	}

	// Phase 3: apply the successful moves to a FRESH copy of each file's
	// metadata (it may have changed during phase 2) and persist it.
	restoredFiles := 0

	for fileID, ts := range moved {
		b.mu.RLock()
		meta, ok := b.fileMeta[fileID]
		if !ok {
			b.mu.RUnlock()
			continue // deleted while we were copying
		}

		newChunkMap := make(map[string]string, len(meta.ChunkMap))
		for k, v := range meta.ChunkMap {
			newChunkMap[k] = v
		}

		newMappings := make([]ShardMapping, len(meta.Mappings))
		copy(newMappings, meta.Mappings)
		b.mu.RUnlock()

		changed := false

		for _, t := range ts {
			// Only apply if the shard is still where we read it from.
			if newChunkMap[t.chunkID] != t.backupNode {
				continue
			}

			newChunkMap[t.chunkID] = nodeID

			for i := range newMappings {
				if fmt.Sprintf("%s_%s", fileID, newMappings[i].ShardID) == t.chunkID {
					newMappings[i].NodeID = nodeID
				}
			}

			changed = true
		}

		if !changed {
			continue
		}

		meta.ChunkMap = newChunkMap
		meta.Mappings = newMappings

		if err := b.saveMeta(fileID, meta); err != nil {
			b.logEvent(fmt.Sprintf("WARNING: restore of %s applied locally but Raft commit failed: %v", fileID, err))
		}

		restoredFiles++
		fmt.Printf("=> RS Restoration: Pulled shards back home to %s for file %s\n", nodeID, fileID)
	}

	// Only a restore that actually moved shards is a "heal"; reviving a node
	// that had nothing rerouted would just pollute the histogram.
	if restoredFiles > 0 {
		healLatencyMs.Observe(float64(time.Since(healStart).Milliseconds()))
	}
}

// startStorageFailoverWatcher is the storage-node equivalent of
// startRaftFailoverWatcher: without it, a storage PC that actually goes
// offline (container stopped, PC unplugged) outside of the manual
// /kill-node endpoint is never detected, and chunkMap keeps pointing at a
// dead node forever - reads still limp along via the primary/backup race in
// fetchShardAdaptive, but new uploads and the dashboard both stay wrong
// until someone notices and hits /kill-node by hand. The streak threshold
// (checks * 5s before acting) is tunable via STORAGE_FAILOVER_STREAK.
func (b *Backend) startStorageFailoverWatcher() {
	streakThreshold := envIntOrDefault("STORAGE_FAILOVER_STREAK", 3)
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		// Same reasoning as the raft watcher's grace period: give every
		// storage container time to actually finish starting before
		// treating a still-booting node as "down".
		time.Sleep(30 * time.Second)

		for range ticker.C {
			b.mu.RLock()
			allNodes := make(map[string]bool, len(b.config.StorageNodes)+len(b.config.BackupNodeAddrs))
			for id := range b.config.StorageNodes {
				allNodes[id] = true
			}
			for id := range b.config.BackupNodeAddrs {
				allNodes[id] = true
			}
			b.mu.RUnlock()

			for nodeID := range allNodes {
				reachable := b.cc.PingNode(nodeID)

				b.mu.Lock()
				manuallyKilled := b.killedNodes[nodeID]
				wasAutoDead := b.storageDeadStreak[nodeID] >= streakThreshold

				if !reachable {
					b.storageDeadStreak[nodeID]++
				} else {
					b.storageDeadStreak[nodeID] = 0
				}
				streak := b.storageDeadStreak[nodeID]
				b.mu.Unlock()

				// Only act automatically on nodes nobody manually
				// kill/revived through the chaos-test endpoint - don't
				// fight a deliberate manual test.
				if manuallyKilled {
					continue
				}

				if streak == streakThreshold {
					fmt.Printf("[StorageWatcher] %s unreachable for %ds, auto-rerouting its shards\n", nodeID, streakThreshold*5)
					b.logEvent(fmt.Sprintf("Auto-failover: %s unreachable for %ds, rerouted its shards to backup", nodeID, streakThreshold*5))
					b.rerouteShardsAwayFrom(nodeID)
				} else if reachable && wasAutoDead {
					fmt.Printf("[StorageWatcher] %s reachable again, restoring its shards\n", nodeID)
					b.logEvent(fmt.Sprintf("Auto-recovery: %s reachable again, restored its shards", nodeID))
					b.restoreShardsHomeTo(nodeID)
				}
			}
		}
	}()
}

func (b *Backend) HandleUpload(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	atomic.AddInt64(&b.uploadRequestsTotal, 1)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Cap the request body, and keep at most 32 MB of the form in RAM
	// (anything bigger spills to temp files that are removed below).
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize+(1<<20))
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, fmt.Sprintf("File exceeds the %d MB limit.", maxUploadSize>>20), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to read uploaded file", http.StatusBadRequest)
		return
	}

	if header.Size > maxUploadSize {
		http.Error(w, fmt.Sprintf("File exceeds the %d MB limit.", maxUploadSize>>20), http.StatusRequestEntityTooLarge)
		return
	}

	defer file.Close()

	rawID := r.FormValue("id")
	if rawID == "" {
		rawID = header.Filename
	}

	fileID := sanitizeFileID(rawID)

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read file contents", http.StatusInternalServerError)
		return
	}

	shards, err := encodeFileToShards(fileBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	shardSize := 0
	if len(shards) > 0 {
		shardSize = len(shards[0])
	}

	ctx := context.Background()

	b.mu.Lock()

	if _, exists := b.fileMeta[fileID]; exists || b.uploadsInFlight[fileID] {
		b.mu.Unlock()
		http.Error(w, fmt.Sprintf("File ID '%s' is already in use.", fileID), http.StatusConflict)
		return
	}

	// Reserve the ID before unlocking so a concurrent upload of the same ID
	// is rejected instead of racing this one.
	b.uploadsInFlight[fileID] = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.uploadsInFlight, fileID)
		b.mu.Unlock()
	}()

	b.beginUploadWAL(fileID)

	// Skip nodes under a simulated partition (/kill-node) so chaos tests
	// behave like a real outage: new shards must not land on a "dead" node.
	var mainNodes, liveBackups []string

	b.mu.RLock()
	for id := range b.config.StorageNodes {
		isBackup := false

		for _, bNode := range b.config.BackupNodes {
			if id == bNode {
				isBackup = true
			}
		}

		if !isBackup && !b.killedNodes[id] {
			mainNodes = append(mainNodes, id)
		}
	}

	for _, id := range b.config.BackupNodes {
		if !b.killedNodes[id] {
			liveBackups = append(liveBackups, id)
		}
	}
	if len(liveBackups) == 0 {
		// Every backup is partitioned: keep the full list so indexing stays
		// valid; the redundancy writes will simply fail and be recorded as
		// "no backup copy".
		liveBackups = append(liveBackups, b.config.BackupNodes...)
	}
	b.mu.RUnlock()

	if len(mainNodes) == 0 || len(liveBackups) == 0 {
		b.commitUploadWAL(fileID)
		http.Error(w, "No reachable storage nodes available", http.StatusServiceUnavailable)
		return
	}

	// Sort to ensure deterministic shard distribution across the physical cluster
	sort.Strings(mainNodes)

	rGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	rGen.Shuffle(len(mainNodes), func(i, j int) {
		mainNodes[i], mainNodes[j] = mainNodes[j], mainNodes[i]
	})

	chunkMap := make(map[string]string)
	backupMap := make(map[string]string)
	homeMap := make(map[string]string)
	checksumMap := make(map[string]string)

	var mappings []ShardMapping
	lostShards := 0 // shards that failed on BOTH home and backup

	for i := 0; i < rsTotalShards; i++ {
		chunkData := shards[i]
		shardID := fmt.Sprintf("shard_%d", i+1)
		fullChunkKey := fmt.Sprintf("%s_%s", fileID, shardID)

		checksum := shardChecksum(chunkData)
		checksumMap[fullChunkKey] = checksum

		homeNode := mainNodes[i%len(mainNodes)]
		backupNode := liveBackups[i%len(liveBackups)]

		targetNode := homeNode

		if err := b.cc.PutShard(
			ctx,
			targetNode,
			fileID,
			uint32(i),
			chunkData,
			checksum,
		); err != nil {
			// FIX: surface the real reason the primary write failed instead of
			// silently swallowing it - this is what tells you WHY every shard
			// is landing on the backup nodes instead of Node-01..06.
			fmt.Printf("[Upload] PutShard to primary %s (%s) failed for %s shard %d: %v\n",
				targetNode, b.config.StorageNodes[targetNode], fileID, i, err)

			targetNode = backupNode

			// FIX: check the backup write's result too - previously this was
			// `_ = b.cc.PutShard(...)`, so if the backup write ALSO failed
			// (e.g. every node's storage path was broken), chunkMap/backupMap
			// still recorded the backup as holding the shard even though
			// nothing was actually written anywhere. That's why the
			// dashboard showed everything "on" Node-07/08 while their disks
			// stayed empty.
			if backupErr := b.cc.PutShard(
				ctx,
				backupNode,
				fileID,
				uint32(i),
				chunkData,
				checksum,
			); backupErr != nil {
				fmt.Printf("[Upload] PutShard to backup %s ALSO failed for %s shard %d: %v - shard is not stored anywhere\n",
					backupNode, fileID, i, backupErr)
				b.logEvent(fmt.Sprintf("CRITICAL: shard %d of %s failed on both primary %s and backup %s - data not stored", i, fileID, homeNode, backupNode))
				lostShards++
				// Nothing was stored: don't record a location for this shard.
				// (Empty node IDs are skipped by fetch, reroute and delete.)
				targetNode = ""
				backupNode = ""
			}
		} else if targetNode != backupNode {
			// Primary succeeded, push a redundancy copy to the backup node.
			// If this fails, do NOT record the backup as holding the shard,
			// otherwise a later reroute would point at a shard that doesn't exist.
			if backupErr := b.cc.PutShard(
				ctx,
				backupNode,
				fileID,
				uint32(i),
				chunkData,
				checksum,
			); backupErr != nil {
				fmt.Printf("[Upload] redundancy copy to backup %s failed for %s shard %d: %v\n",
					backupNode, fileID, i, backupErr)
				b.logEvent(fmt.Sprintf("WARNING: shard %d of %s has no backup copy (backup %s write failed)", i, fileID, backupNode))
				backupNode = ""
			}
		}

		chunkMap[fullChunkKey] = targetNode
		backupMap[fullChunkKey] = backupNode
		homeMap[fullChunkKey] = homeNode

		mappings = append(
			mappings,
			ShardMapping{
				ShardID: shardID,
				NodeID:  targetNode,
			},
		)
	}

	// Reed-Solomon can only rebuild up to rsParityShards missing shards.
	// Beyond that the file is unrecoverable, so abort instead of
	// reporting success for data that isn't stored.
	if lostShards > rsParityShards {
		for key, node := range chunkMap {
			idx := shardIndexOf(strings.TrimPrefix(key, fileID+"_"))
			if node != "" {
				_ = b.cc.DeleteShard(ctx, node, fileID, idx)
			}
			if bn := backupMap[key]; bn != "" && bn != node {
				_ = b.cc.DeleteShard(ctx, bn, fileID, idx)
			}
		}
		b.commitUploadWAL(fileID)
		b.logEvent(fmt.Sprintf("Upload of %s aborted: %d shards failed on both home and backup (max recoverable: %d)", fileID, lostShards, rsParityShards))
		http.Error(w, fmt.Sprintf("Upload failed: %d shards could not be stored (max recoverable: %d)", lostShards, rsParityShards), http.StatusServiceUnavailable)
		return
	}
	if lostShards > 0 {
		b.logEvent(fmt.Sprintf("WARNING: %s stored with %d lost shard(s); no redundancy margin left for those", fileID, lostShards))
	}

	meta := FileMeta{
		ID:           fileID,
		Name:         header.Filename,
		Size:         int64(len(fileBytes)),
		Mappings:     mappings,
		ChunkMap:     chunkMap,
		BackupMap:    backupMap,
		HomeMap:      homeMap,
		DataShards:   rsDataShards,
		ParityShards: rsParityShards,
		ShardSize:    shardSize,
		ChecksumMap:  checksumMap,
		LastAccessed: time.Now().Unix(),
		UploadedAt:   time.Now().Unix(),
	}

	if err := b.saveMetaRaftFirst(fileID, meta); err != nil {
		// Raft-first means nothing was recorded locally. Remove the shards
		// this upload wrote and clear the WAL entry, then report failure.
		for key, node := range chunkMap {
			idx := shardIndexOf(strings.TrimPrefix(key, fileID+"_"))
			if node != "" {
				_ = b.cc.DeleteShard(ctx, node, fileID, idx)
			}
			if bn := backupMap[key]; bn != "" && bn != node {
				_ = b.cc.DeleteShard(ctx, bn, fileID, idx)
			}
		}
		b.commitUploadWAL(fileID)
		b.logEvent(fmt.Sprintf("Upload of %s failed: Raft commit failed: %v", fileID, err))
		http.Error(w, "Consensus failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	b.commitUploadWAL(fileID)
	atomic.AddInt64(&b.uploadSuccessTotal, 1)
	// Latency is recorded for successful uploads only, so failed/rejected
	// requests don't mix into the same histogram.
	b.uploadLatencyMs.Observe(float64(time.Since(start).Milliseconds()))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Upload successful"))
}

func (b *Backend) HandleFiles(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := []FileMeta{}

	for _, meta := range b.fileMeta {
		list = append(list, meta)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func (b *Backend) fetchShardAdaptive(
	ctx context.Context,
	fileID string,
	shardIdx uint32,
	checksum string,
	nodeIDs ...string,
) []byte {
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		data []byte
		ok   bool
	}

	resCh := make(chan result, len(nodeIDs))
	var wg sync.WaitGroup

	for _, nodeID := range nodeIDs {
		if nodeID == "" || b.isNodeKilled(nodeID) {
			resCh <- result{}
			continue
		}

		wg.Add(1)

		go func(nodeID string) {
			defer wg.Done()

			fetchStart := time.Now()
			data, _, err := b.cc.GetShard(
				raceCtx,
				nodeID,
				fileID,
				shardIdx,
			)

			// If the race was already decided by another goroutine, this
			// fetch was cancelled out from under us - don't count it as a
			// genuine failure (or a latency sample), it just lost the race.
			if raceCtx.Err() != nil {
				resCh <- result{}
				return
			}

			shardFetchLatencyMs.Observe(float64(time.Since(fetchStart).Microseconds()) / 1000.0)

			if err != nil || len(data) == 0 || !verifyChecksum(data, checksum) {
				atomic.AddInt64(&b.shardFetchFailTotal, 1)
				resCh <- result{}
				return
			}

			atomic.AddInt64(&b.shardFetchSuccessTotal, 1)
			resCh <- result{
				data: data,
				ok:   true,
			}
		}(nodeID)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	for r := range resCh {
		if r.ok {
			cancel()
			return r.data
		}
	}

	return nil
}

func (b *Backend) downloadFileAdaptive(
	fileID string,
	countStats bool,
) ([]byte, string, error) {
	if countStats {
		atomic.AddInt64(&b.sfTotal, 1)
	}

	atomic.AddInt64(&b.sfInflight, 1)
	defer atomic.AddInt64(&b.sfInflight, -1)

	fileOut, name, err, shared := b.sfGroup.Do(fileID, func() ([]byte, string, error) {
		b.mu.RLock()
		meta, exists := b.fileMeta[fileID]
		b.mu.RUnlock()

		if !exists {
			return nil, "", fmt.Errorf("file not found")
		}

		ctx, cancelAll := context.WithCancel(context.Background())
		defer cancelAll()

		shards := make([][]byte, rsTotalShards)

		type shardResult struct {
			idx  int
			data []byte
		}

		resCh := make(chan shardResult, len(meta.Mappings))

		for i, mapping := range meta.Mappings {
			idx := i

			chunkID := fmt.Sprintf("%s_%s", fileID, mapping.ShardID)
			shardIdx := shardIndexOf(mapping.ShardID)
			primary := meta.ChunkMap[chunkID]
			backup := meta.BackupMap[chunkID]
			checksum := meta.ChecksumMap[chunkID]

			go func() {
				data := b.fetchShardAdaptive(
					ctx,
					fileID,
					shardIdx,
					checksum,
					primary,
					backup,
				)

				resCh <- shardResult{
					idx:  idx,
					data: data,
				}
			}()
		}

		successes, received := 0, 0

		for received < len(meta.Mappings) {
			r := <-resCh
			received++

			shards[r.idx] = r.data

			if r.data != nil {
				successes++

				if successes >= meta.DataShards {
					cancelAll()
					break
				}
			}
		}

		// Reconstruction is only "needed" when a data shard is missing; the
		// early exit above leaves parity shards nil on purpose, so only look
		// at the data shards.
		needsReconstruction := false
		for i := 0; i < meta.DataShards && i < len(shards); i++ {
			if shards[i] == nil {
				needsReconstruction = true
				break
			}
		}

		fileOut, err := reconstructAndJoin(shards, int(meta.Size))
		if err != nil {
			return nil, "", err
		}

		if needsReconstruction {
			atomic.AddInt64(&b.reconstructTotal, 1)
		}

		return fileOut, meta.Name, nil
	})

	if shared && countStats {
		atomic.AddInt64(&b.sfDeduped, 1)
	}

	return fileOut, name, err
}

func (b *Backend) HandleStats(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	totalFiles := len(b.fileMeta)
	b.mu.RUnlock()

	stats := map[string]interface{}{
		"total_files":       totalFiles,
		"total_views":       atomic.LoadInt64(&b.sfViews),
		"download_requests": atomic.LoadInt64(&b.sfTotal),
		"deduped_requests":  atomic.LoadInt64(&b.sfDeduped),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (b *Backend) HandleDownload(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	fileID := r.URL.Query().Get("id")

	if fileID == "" {
		fileID = r.URL.Query().Get("file")
	}

	if fileOut, name, err := b.downloadFileAdaptive(fileID, true); err == nil {
		// mime.FormatMediaType quotes/encodes the name, so a filename with
		// quotes or newlines can't inject extra header content.
		cd := mime.FormatMediaType("attachment", map[string]string{"filename": name})
		if cd == "" {
			cd = "attachment"
		}
		w.Header().Set("Content-Disposition", cd)
		w.Header().Set("X-Content-Type-Options", "nosniff")

		w.Header().Set(
			"Content-Type",
			"application/octet-stream",
		)

		w.Write(fileOut)
		atomic.AddInt64(&b.downloadSuccessTotal, 1)
		b.downloadLatencyMs.Observe(float64(time.Since(start).Milliseconds()))
		b.touchLastAccessed(fileID)
		return
	}

	http.Error(w, "Failed to download file", http.StatusInternalServerError)
}

func (b *Backend) HandleView(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")

	if fileID == "" {
		fileID = r.URL.Query().Get("file")
	}

	atomic.AddInt64(&b.sfViews, 1)

	if fileOut, name, err := b.downloadFileAdaptive(fileID, false); err == nil {
		ext := strings.ToLower(filepath.Ext(name))
		var ct string

		// 1. Force code and config files to be treated as plain text by the browser
		switch ext {
		case ".yml", ".yaml", ".json", ".md", ".csv", ".go", ".py", ".js", ".sh", ".txt":
			ct = "text/plain; charset=utf-8"

		default:
			// 2. Fallback to OS registry for things like .pdf, .jpg, .png
			ct = mime.TypeByExtension(ext)

			// 3. Sniff bytes if all else fails
			if ct == "" || ct == "application/octet-stream" {
				ct = http.DetectContentType(fileOut)
			}
		}

		// HTML/SVG/XML can run scripts on this origin (stored XSS), so they are
		// never shown inline - the browser downloads them instead.
		disposition := "inline"
		lowerCT := strings.ToLower(ct)
		for _, risky := range []string{"text/html", "application/xhtml", "image/svg", "text/xml", "application/xml"} {
			if strings.HasPrefix(lowerCT, risky) {
				disposition = "attachment"
				ct = "application/octet-stream"
				break
			}
		}

		cd := mime.FormatMediaType(disposition, map[string]string{"filename": name})
		if cd == "" {
			cd = disposition
		}
		w.Header().Set("Content-Disposition", cd)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Type", ct)
		w.Write(fileOut)
		b.touchLastAccessed(fileID)
		return
	}

	http.Error(w, "Failed to view file", http.StatusInternalServerError)
}

var errMetaNotFound = errors.New("file not found")

// updateMeta changes a file's metadata without clobbering concurrent changes.
// mutate must set ABSOLUTE values (not toggle or increment) because it runs
// twice: once on a snapshot that goes to Raft, and again on whatever copy is
// current after the commit, so a reroute/restore that landed during the
// (slow) Raft round trip keeps its ChunkMap/Mappings instead of being
// overwritten by a stale copy. mutate returns false when nothing needs to
// change, in which case no Raft write happens.
func (b *Backend) updateMeta(fileID string, mutate func(*FileMeta) bool) (bool, error) {
	b.mu.RLock()
	meta, ok := b.fileMeta[fileID]
	b.mu.RUnlock()

	if !ok {
		return false, errMetaNotFound
	}

	if !mutate(&meta) {
		return false, nil
	}

	payload, err := json.Marshal(meta)
	if err != nil {
		return false, fmt.Errorf("marshal meta: %w", err)
	}

	if err := b.raftApply(LogCommand{Operation: "SET_META", Payload: payload}); err != nil {
		fmt.Printf("[Raft] SET_META failed for %s: %v\n", fileID, err)
		return false, fmt.Errorf("raft commit failed: %w", err)
	}

	b.mu.Lock()
	cur, ok := b.fileMeta[fileID]
	if !ok {
		b.mu.Unlock()
		return false, errMetaNotFound // deleted while we were committing
	}
	mutate(&cur)
	b.fileMeta[fileID] = cur
	curBytes, _ := json.Marshal(cur)
	b.mu.Unlock()

	_ = b.db.Set([]byte("file:"+fileID), curBytes, pebble.Sync)
	return true, nil
}

// lastAccessedGranularity is how stale LastAccessed may get before a view or
// download refreshes it. The only consumer is the 30-day TTL sweeper, so hour
// resolution is plenty - and it keeps reads from becoming Raft writes.
const lastAccessedGranularity = int64(60 * 60)

func (b *Backend) touchLastAccessed(fileID string) {
	now := time.Now().Unix()

	_, err := b.updateMeta(fileID, func(m *FileMeta) bool {
		if now-m.LastAccessed < lastAccessedGranularity {
			return false
		}
		m.LastAccessed = now
		return true
	})
	if err != nil && !errors.Is(err, errMetaNotFound) {
		fmt.Printf("[Raft] last-accessed update for %s not committed: %v\n", fileID, err)
	}
}

func contentTypeFor(name, disposition string) string {
	if disposition != "inline" {
		return "application/octet-stream"
	}

	if ct := mime.TypeByExtension(filepath.Ext(name)); ct != "" {
		return ct
	}

	return "application/octet-stream"
}

func (b *Backend) HandleDelete(w http.ResponseWriter, r *http.Request) {
	// GET must never delete (an <img src="/delete?id=..."> on any web page
	// would trigger it). The dashboard already uses DELETE.
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}

	fileID := r.URL.Query().Get("id")

	if fileID == "" {
		fileID = r.URL.Query().Get("file")
	}

	b.deleteFilePermanently(fileID)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Deleted successfully"))
}

func (b *Backend) deleteFilePermanently(fileID string) {
	ctx := context.Background()

	b.mu.RLock()

	meta, exists := b.fileMeta[fileID]

	b.mu.RUnlock()

	if exists {
		for _, mapping := range meta.Mappings {
			chunkID := fmt.Sprintf("%s_%s", fileID, mapping.ShardID)
			shardIdx := shardIndexOf(mapping.ShardID)

			// Delete from every node that may hold a copy: current location,
			// backup, and the original home (a rerouted shard leaves a stale
			// copy there). Skip empty IDs and duplicates.
			seen := map[string]bool{"": true}
			for _, node := range []string{
				meta.ChunkMap[chunkID],
				meta.BackupMap[chunkID],
				meta.HomeMap[chunkID],
			} {
				if seen[node] {
					continue
				}
				seen[node] = true
				_ = b.cc.DeleteShard(ctx, node, fileID, shardIdx)
			}
		}

		_ = b.db.Delete(
			[]byte("file:"+fileID),
			pebble.Sync,
		)

		b.mu.Lock()
		delete(b.fileMeta, fileID)
		b.mu.Unlock()

		cmd := LogCommand{
			Operation: "DELETE_META",
			Payload:   []byte(fileID),
		}

		if err := b.raftApply(cmd); err != nil {
			fmt.Printf("[Raft] DELETE_META failed for %s: %v\n", fileID, err)
		}
	}
}

// startUnifiedSweeper now ONLY runs the 30-day TTL check.
// Hardware capacity monitoring is handled independently by each physical PC's background agent.
func (b *Backend) startUnifiedSweeper() {
	ticker := time.NewTicker(5 * time.Minute)

	go func() {
		for range ticker.C {
			now := time.Now().Unix()
			thirtyDays := int64(30 * 24 * 60 * 60)

			var timeExpiredFiles []string

			b.mu.RLock()

			for id, meta := range b.fileMeta {
				if meta.IsPinned {
					continue
				}

				if (now - meta.LastAccessed) > thirtyDays {
					timeExpiredFiles = append(timeExpiredFiles, id)
				}
			}

			b.mu.RUnlock()

			for _, fileID := range timeExpiredFiles {
				b.deleteFilePermanently(fileID)
				atomic.AddInt64(&b.sweeperEvictionsTotal, 1)

				fmt.Printf(
					"[Sweeper] Evicted abandoned 30-day file over network: %s\n",
					fileID,
				)
			}
		}
	}()
}

func (b *Backend) HandleTogglePin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}

	fileID := r.URL.Query().Get("id")

	b.mu.RLock()
	current, exists := b.fileMeta[fileID]
	b.mu.RUnlock()

	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Decide the target once, then set it absolutely (see updateMeta).
	target := !current.IsPinned
	_, err := b.updateMeta(fileID, func(m *FileMeta) bool {
		if m.IsPinned == target {
			return false
		}
		m.IsPinned = target
		return true
	})
	if errors.Is(err, errMetaNotFound) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Consensus failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// HandleCacheInspect completely replaces local memory reads with physical gRPC calls to the storage PCs
func (b *Backend) HandleCacheInspect(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()

	// Create a local map of clients to avoid locking the orchestrator during network calls
	clients := make(map[string]pb.CacheServiceClient)

	for k, v := range b.cc.cacheClients {
		clients[k] = v
	}

	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)

	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex

	// The frontend expects a JSON object where the keys are Node IDs
	results := make(map[string]interface{})

	for nodeID, client := range clients {
		wg.Add(1)

		go func(id string, c pb.CacheServiceClient) {
			defer wg.Done()

			// Send network gRPC request directly to the physical PC's RAM engine
			resp, err := c.InspectCache(
				ctx,
				&pb.InspectRequest{},
			)

			if err == nil {
				mu.Lock()

				results[id] = map[string]interface{}{
					"a1in":  resp.A1In,
					"am":    resp.Am,
					"a1out": resp.A1Out,
				}

				mu.Unlock()
			}
		}(nodeID, client)
	}

	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (b *Backend) logEvent(msg string) {
	b.eventsMu.Lock()
	defer b.eventsMu.Unlock()

	b.events = append(
		b.events,
		LogEvent{
			Message: msg,
			Time:    time.Now().Unix(),
		},
	)

	if len(b.events) > 100 {
		b.events = b.events[len(b.events)-100:]
	}
}

func (b *Backend) HandleEvents(w http.ResponseWriter, r *http.Request) {
	b.eventsMu.Lock()

	out := make([]LogEvent, len(b.events))
	copy(out, b.events)

	b.eventsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (b *Backend) HandleCacheStats(w http.ResponseWriter, r *http.Request) {
	var totalMem, totalDisk, totalMiss int64

	b.mu.RLock()

	// Create a local copy of the active gRPC clients to avoid holding the lock during network calls
	clients := make(map[string]pb.CacheServiceClient)

	for k, v := range b.cc.cacheClients {
		clients[k] = v
	}

	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)

	defer cancel()

	// Fetch stats concurrently from all physical storage nodes
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, client := range clients {
		wg.Add(1)

		go func(c pb.CacheServiceClient) {
			defer wg.Done()

			resp, err := c.GetCacheStats(
				ctx,
				&pb.StatsRequest{},
			)

			if err == nil {
				mu.Lock()

				totalMem += resp.MemHits
				totalDisk += resp.DiskHits
				totalMiss += resp.Misses

				mu.Unlock()
			}
		}(client)
	}

	wg.Wait()

	// Calculate the global 2Q cache hit rate
	hitRate := 0.0
	totalRequests := totalMem + totalDisk + totalMiss

	if totalRequests > 0 {
		hitRate = (float64(totalMem) / float64(totalRequests)) * 100.0
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(map[string]interface{}{
		"total": map[string]int64{
			"mem_hits":  totalMem,
			"disk_hits": totalDisk,
			"misses":    totalMiss,
		},
		"hit_rate_pct": hitRate,
	})
}