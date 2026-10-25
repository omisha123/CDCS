package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "cdcs1/cdcspb"
	"github.com/cockroachdb/pebble"
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

	// Raft standby auto-promotion bookkeeping.
	raftDeadStreak map[string]int  // main raft node ID -> consecutive dead health-check count
	raftPromoted   map[string]bool // main raft node ID -> already had a standby promoted in its place
	usedBackups    map[string]bool // standby raft node ID -> already promoted (don't reuse)

	// Storage-node auto-failover bookkeeping (mirrors the raft one above).
	storageDeadStreak map[string]int // storage node ID -> consecutive unreachable health-check count
}

type LogEvent struct {
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

var fileIDSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

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

	db, err := pebble.Open("metadata.db", &pebble.Options{})
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
		raftDeadStreak:    make(map[string]int),
		raftPromoted:      make(map[string]bool),
		usedBackups:       make(map[string]bool),
		storageDeadStreak: make(map[string]int),
	}

	b.loadMetadataFromDB()
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

// saveMeta persists meta locally for the dashboard, then broadcasts to the physical Raft network.
func (b *Backend) saveMeta(fileID string, meta FileMeta) error {
	metaBytes, _ := json.Marshal(meta)

	// 1. Save locally to Orchestrator's Pebble DB
	_ = b.db.Set([]byte("file:"+fileID), metaBytes, pebble.Sync)

	b.mu.Lock()
	b.fileMeta[fileID] = meta
	b.mu.Unlock()

	cmd := LogCommand{
		Operation: "SET_META",
		Payload:   metaBytes,
	}

	if err := b.raftApply(cmd); err != nil {
		fmt.Printf("[Raft] SET_META failed for %s: %v\n", fileID, err)
		return err
	}

	return nil
}

func (b *Backend) raftApply(cmd LogCommand) error {
	b.mu.RLock()
	raftNodes := make([]string, 0, len(b.config.RaftNodes))

	for _, addr := range b.config.RaftNodes {
		raftNodes = append(raftNodes, addr)
	}

	b.mu.RUnlock()

	var lastErr error

	for _, raftAddr := range raftNodes {
		apiAddr := deriveAPIAddr(raftAddr)

		if err := SubmitLogCommand(apiAddr, cmd); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	if lastErr == nil {
		return fmt.Errorf("no Raft nodes configured")
	}

	return lastErr
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
	var statuses []NodeStatus

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

	for id, addr := range allNodes {
		status := "ONLINE"

		// If chaos test simulated a kill OR the physical gRPC connection is actually dead
		if b.killedNodes[id] || !b.cc.PingNode(id) {
			status = "OFFLINE"
		}

		statuses = append(statuses, NodeStatus{
			ID:       id,
			Addr:     addr,
			Status:   status,
			IsBackup: backupSet[id],
		})
	}

	b.mu.RUnlock()

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
	for id, addr := range b.config.RaftNodes {
		raftNodes[id] = addr
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
				}
			}

			mu.Lock()
			results[nodeID] = statusEntry
			mu.Unlock()
		}(id, addr)
	}

	wg.Wait()
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
			b.mu.Unlock()
			b.logEvent(fmt.Sprintf("Promoted standby %s to full raft voter", nodeID))
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "%s promoted to voter", nodeID)
			return
		} else {
			lastErr = err
		}
	}

	http.Error(w, fmt.Sprintf("promote failed: %v", lastErr), http.StatusInternalServerError)
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

			raftNodes := make([]string, 0, len(b.config.RaftNodes))
			for _, a := range b.config.RaftNodes {
				raftNodes = append(raftNodes, a)
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

			b.mu.Lock()
			if promoted {
				b.raftPromoted[deadNode] = true
				b.usedBackups[standbyID] = true
				b.mu.Unlock()
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
	b.killedNodes[nodeID] = true
	b.mu.Unlock()

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
		b.saveMeta(fileID, meta)
		fmt.Printf("=> RS Failover: Rerouted shards for file %s away from %s\n", fileID, nodeID)
	}
}

// ----------------------------------------------------------------------
// FULLY NETWORKED REVIVE NODE (Network Restoration)
// ----------------------------------------------------------------------

func (b *Backend) HandleReviveNode(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")

	b.mu.Lock()
	delete(b.killedNodes, nodeID)
	b.mu.Unlock()

	fmt.Printf("=> Restoring Network Path to %s\n", nodeID)
	b.restoreShardsHomeTo(nodeID)

	w.WriteHeader(http.StatusOK)
}

// restoreShardsHomeTo pulls shards back onto nodeID from wherever they got
// rerouted to, once nodeID is confirmed reachable again. Shared by the
// manual /revive-node endpoint and the automatic storage watcher below.
func (b *Backend) restoreShardsHomeTo(nodeID string) {
	ctx := context.Background()

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

			if meta.HomeMap[chunkID] == nodeID && newChunkMap[chunkID] != nodeID {
				shardUint := shardIndexOf(mapping.ShardID)
				backupNode := newChunkMap[chunkID]

				data, checksum, err := b.cc.GetShard(ctx, backupNode, fileID, shardUint)
				if err != nil {
					fmt.Printf("=> RS Restoration: could not read %s shard %d from %s to restore to %s: %v\n", fileID, shardUint, backupNode, nodeID, err)
					continue
				}

				if storeErr := b.cc.PutShard(ctx, nodeID, fileID, shardUint, data, checksum); storeErr != nil {
					fmt.Printf("=> RS Restoration: failed writing %s shard %d back to %s: %v\n", fileID, shardUint, nodeID, storeErr)
					continue
				}

				newChunkMap[chunkID] = nodeID
				newMappings[i].NodeID = nodeID
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
		b.saveMeta(fileID, meta)
		fmt.Printf("=> RS Restoration: Pulled shards back home to %s for file %s\n", nodeID, fileID)
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(1000 << 20); err != nil {
		http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to read uploaded file", http.StatusBadRequest)
		return
	}

	if header.Size > 1000<<20 {
		http.Error(w, "File exceeds the 1GB limit.", http.StatusRequestEntityTooLarge)
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

	if _, exists := b.fileMeta[fileID]; exists {
		b.mu.Unlock()
		http.Error(w, fmt.Sprintf("File ID '%s' is already in use.", fileID), http.StatusConflict)
		return
	}

	b.mu.Unlock()

	b.beginUploadWAL(fileID)

	var mainNodes []string

	for id := range b.config.StorageNodes {
		isBackup := false

		for _, bNode := range b.config.BackupNodes {
			if id == bNode {
				isBackup = true
			}
		}

		if !isBackup {
			mainNodes = append(mainNodes, id)
		}
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

	for i := 0; i < rsTotalShards; i++ {
		chunkData := shards[i]
		shardID := fmt.Sprintf("shard_%d", i+1)
		fullChunkKey := fmt.Sprintf("%s_%s", fileID, shardID)

		checksum := shardChecksum(chunkData)
		checksumMap[fullChunkKey] = checksum

		homeNode := mainNodes[i%len(mainNodes)]
		backupNode := b.config.BackupNodes[i%len(b.config.BackupNodes)]

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
			}
		} else if targetNode != backupNode {
			// Primary succeeded, push a redundancy copy to the backup node
			_ = b.cc.PutShard(
				ctx,
				backupNode,
				fileID,
				uint32(i),
				chunkData,
				checksum,
			)
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

	if err := b.saveMeta(fileID, meta); err != nil {
		b.deleteFilePermanently(fileID) // roll back shards + local meta
		b.commitUploadWAL(fileID)
		http.Error(w, "Consensus failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	b.commitUploadWAL(fileID)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Upload successful"))
}
func (b *Backend) isKilled(id string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.killedNodes[id]
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
		if nodeID == "" || b.isKilled(nodeID) {
			resCh <- result{}
			continue
		}

		wg.Add(1)

		go func(nodeID string) {
			defer wg.Done()

			data, _, err := b.cc.GetShard(
				raceCtx,
				nodeID,
				fileID,
				shardIdx,
			)

			if err != nil || len(data) == 0 || !verifyChecksum(data, checksum) {
				resCh <- result{}
				return
			}

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

		fileOut, err := reconstructAndJoin(shards, int(meta.Size))
		if err != nil {
			return nil, "", err
		}

		return fileOut, meta.Name, nil
	})

	if shared && countStats {
		atomic.AddInt64(&b.sfDeduped, 1)
	}

	return fileOut, name, err
}

func (b *Backend) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"total_files":       len(b.fileMeta),
		"total_views":       atomic.LoadInt64(&b.sfViews),
		"download_requests": atomic.LoadInt64(&b.sfTotal),
		"deduped_requests":  atomic.LoadInt64(&b.sfDeduped),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (b *Backend) HandleDownload(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")

	if fileID == "" {
		fileID = r.URL.Query().Get("file")
	}

	if fileOut, name, err := b.downloadFileAdaptive(fileID, true); err == nil {
		w.Header().Set(
			"Content-Disposition",
			fmt.Sprintf("attachment; filename=\"%s\"", name),
		)

		w.Header().Set(
			"Content-Type",
			"application/octet-stream",
		)

		w.Write(fileOut)
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
		w.Header().Set(
			"Content-Disposition",
			fmt.Sprintf("inline; filename=\"%s\"", name),
		)

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

		w.Header().Set("Content-Type", ct)
		w.Write(fileOut)
		b.touchLastAccessed(fileID)
		return
	}

	http.Error(w, "Failed to view file", http.StatusInternalServerError)
}

const touchInterval = int64(3600) // 30-day sweeper, so 1h resolution is enough

func (b *Backend) touchLastAccessed(fileID string) {
	now := time.Now().Unix()

	b.mu.Lock()
	meta, exists := b.fileMeta[fileID]
	if !exists || now-meta.LastAccessed < touchInterval {
		b.mu.Unlock()
		return
	}
	meta.LastAccessed = now
	b.fileMeta[fileID] = meta
	b.mu.Unlock()

	go func() {
		b.mu.RLock()
		latest, ok := b.fileMeta[fileID]
		b.mu.RUnlock()
		if !ok {
			return // deleted meanwhile
		}
		_ = b.saveMeta(fileID, latest)
	}()
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

			_ = b.cc.DeleteShard(
				ctx,
				meta.ChunkMap[chunkID],
				fileID,
				shardIdx,
			)

			_ = b.cc.DeleteShard(
				ctx,
				meta.BackupMap[chunkID],
				fileID,
				shardIdx,
			)
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

				fmt.Printf(
					"[Sweeper] Evicted abandoned 30-day file over network: %s\n",
					fileID,
				)
			}
		}
	}()
}

func (b *Backend) HandleTogglePin(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")

	b.mu.RLock()

	meta, exists := b.fileMeta[fileID]

	b.mu.RUnlock()

	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	meta.IsPinned = !meta.IsPinned
	_ = b.saveMeta(fileID, meta)

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
