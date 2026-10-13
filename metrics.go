package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	pb "cdcs1/cdcspb"

	"github.com/shirou/gopsutil/v3/process"
)

type Histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

func NewHistogram(buckets []float64) *Histogram {
	return &Histogram{
		buckets: buckets,
		counts:  make([]int64, len(buckets)),
	}
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sum += v
	h.count++

	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

func (h *Histogram) WriteProm(w http.ResponseWriter, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, b := range h.buckets {
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, h.counts[i])
	}

	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, h.count)
	fmt.Fprintf(w, "%s_sum %g\n", name, h.sum)
	fmt.Fprintf(w, "%s_count %d\n", name, h.count)
}

var (
	// cacheHitLatencyMs / cacheMissLatencyMs are observed inside each
	// storage node's GetShard (server.go) and shipped to the orchestrator
	// through GetCacheStats, which adds them up across nodes.
	cacheHitLatencyMs = NewHistogram([]float64{
		0.1, 0.5, 1, 2, 5, 10, 25, 50, 100,
	})

	cacheMissLatencyMs = NewHistogram([]float64{
		0.1, 0.5, 1, 2, 5, 10, 25, 50, 100,
	})

	shardFetchLatencyMs = NewHistogram([]float64{
		1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
	})

	leaderElectionLatencyMs = NewHistogram([]float64{
		10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
	})

	healLatencyMs = NewHistogram([]float64{
		10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000,
	})

	metadataConsistencyChecksTotal int64
	metadataConsistencyPassTotal   int64
	metadataConsistencyFailTotal   int64

	byteMatchChecksTotal int64
	byteMatchPassTotal   int64
	byteMatchFailTotal   int64

	reconstructionTestsTotal   int64
	reconstructionSuccessTotal int64
	reconstructionFailureTotal int64

	stampedeRequestsTotal       int64
	stampedeStorageFetchesTotal int64

	evictionChecksTotal int64
	evictionPassTotal   int64
	evictionFailTotal   int64
)

func ObserveCacheHitLatency(v float64) {
	cacheHitLatencyMs.Observe(v)
}

func ObserveCacheMissLatency(v float64) {
	cacheMissLatencyMs.Observe(v)
}

func ObserveMetadataConsistency(checkPassed bool) {
	atomic.AddInt64(&metadataConsistencyChecksTotal, 1)

	if checkPassed {
		atomic.AddInt64(&metadataConsistencyPassTotal, 1)
	} else {
		atomic.AddInt64(&metadataConsistencyFailTotal, 1)
	}
}

func ObserveByteMatch(matches bool) {
	atomic.AddInt64(&byteMatchChecksTotal, 1)

	if matches {
		atomic.AddInt64(&byteMatchPassTotal, 1)
	} else {
		atomic.AddInt64(&byteMatchFailTotal, 1)
	}
}

func ObserveReconstructionTest(success bool) {
	atomic.AddInt64(&reconstructionTestsTotal, 1)

	if success {
		atomic.AddInt64(&reconstructionSuccessTotal, 1)
	} else {
		atomic.AddInt64(&reconstructionFailureTotal, 1)
	}
}

func ObserveStampede(requests, storageFetches int64) {
	atomic.AddInt64(&stampedeRequestsTotal, requests)
	atomic.AddInt64(&stampedeStorageFetchesTotal, storageFetches)
}

func ObserveEvictionCheck(passed bool) {
	atomic.AddInt64(&evictionChecksTotal, 1)

	if passed {
		atomic.AddInt64(&evictionPassTotal, 1)
	} else {
		atomic.AddInt64(&evictionFailTotal, 1)
	}
}

// directorySize is used by each storage node (server.go) to report how many
// bytes its own storage directory holds.
func directorySize(root string) int64 {
	var total int64

	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err == nil {
			total += info.Size()
		}

		return nil
	})

	return total
}

func (b *Backend) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// ============================================================
	// SNAPSHOT SHARED STATE (one short lock; network calls happen after)
	// ============================================================

	b.mu.RLock()

	nodeIDs := make([]string, 0, len(b.config.StorageNodes)+len(b.config.BackupNodeAddrs))
	for id := range b.config.StorageNodes {
		nodeIDs = append(nodeIDs, id)
	}
	for id := range b.config.BackupNodeAddrs {
		nodeIDs = append(nodeIDs, id)
	}

	backupSet := make(map[string]bool, len(b.config.BackupNodes))
	for _, id := range b.config.BackupNodes {
		backupSet[id] = true
	}

	killed := make(map[string]bool, len(b.killedNodes))
	for id, v := range b.killedNodes {
		killed[id] = v
	}

	raftBackupNodes := append([]string(nil), b.config.RaftBackupNodes...)
	raftBackupAddrs := make(map[string]string, len(b.config.RaftBackupAddrs))
	for id, addr := range b.config.RaftBackupAddrs {
		raftBackupAddrs[id] = addr
	}

	clients := make(map[string]pb.CacheServiceClient, len(b.cc.cacheClients))
	for id, c := range b.cc.cacheClients {
		clients[id] = c
	}

	fileCount := len(b.fileMeta)
	totalShards := 0
	originalBytes := int64(0)

	for _, meta := range b.fileMeta {
		totalShards += len(meta.Mappings)
		originalBytes += meta.Size
	}

	b.mu.RUnlock()

	// ============================================================
	// GATHER LIVE DATA IN PARALLEL
	//   - storage node reachability
	//   - per-node cache/storage stats over gRPC (GetCacheStats)
	//   - active raft node status over HTTP
	//   - standby raft node reachability
	// ============================================================

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex

	nodeUp := make(map[string]int, len(nodeIDs))
	for _, id := range nodeIDs {
		wg.Add(1)

		go func(nodeID string) {
			defer wg.Done()

			up := 0
			if !killed[nodeID] && b.cc.PingNode(nodeID) {
				up = 1
			}

			mu.Lock()
			nodeUp[nodeID] = up
			mu.Unlock()
		}(id)
	}

	nodeStats := make(map[string]*pb.StatsResponse, len(clients))
	for id, c := range clients {
		wg.Add(1)

		go func(nodeID string, client pb.CacheServiceClient) {
			defer wg.Done()

			resp, err := client.GetCacheStats(ctx, &pb.StatsRequest{})
			if err != nil {
				return
			}

			mu.Lock()
			nodeStats[nodeID] = resp
			mu.Unlock()
		}(id, c)
	}

	var raftLive map[string]map[string]interface{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		raftLive = b.fetchRaftStatuses()
	}()

	standbyUp := make(map[string]int, len(raftBackupNodes))
	for _, id := range raftBackupNodes {
		wg.Add(1)

		go func(nodeID string) {
			defer wg.Done()

			up := 0
			if addr := raftBackupAddrs[nodeID]; addr != "" {
				client := http.Client{Timeout: 2 * time.Second}
				if resp, err := client.Get("http://" + deriveAPIAddr(addr) + "/status"); err == nil {
					resp.Body.Close()
					up = 1
				}
			}

			mu.Lock()
			standbyUp[nodeID] = up
			mu.Unlock()
		}(id)
	}

	wg.Wait()

	// ============================================================
	// STORAGE NODE HEALTH
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_node_up Whether a storage node is currently reachable")
	fmt.Fprintln(w, "# TYPE cdcs_node_up gauge")

	for id, up := range nodeUp {
		fmt.Fprintf(
			w,
			"cdcs_node_up{node=%q,backup=%q} %d\n",
			id,
			boolStr(backupSet[id]),
			up,
		)
	}

	// ============================================================
	// RAFT
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_raft_up Whether a Raft node is currently reachable")
	fmt.Fprintln(w, "# TYPE cdcs_raft_up gauge")

	for id, entry := range raftLive {
		up := 1
		if entry["state"] == "Dead" {
			up = 0
		}

		fmt.Fprintf(
			w,
			"cdcs_raft_up{raft_node=%q,backup=\"false\"} %d\n",
			id,
			up,
		)
	}

	for id, up := range standbyUp {
		fmt.Fprintf(
			w,
			"cdcs_raft_up{raft_node=%q,backup=\"true\"} %d\n",
			id,
			up,
		)
	}

	fmt.Fprintln(w, "# HELP cdcs_raft_is_leader Whether this Raft node is the current cluster leader")
	fmt.Fprintln(w, "# TYPE cdcs_raft_is_leader gauge")

	for id, entry := range raftLive {
		isLeader := 0
		if entry["state"] == "Leader" {
			isLeader = 1
		}

		fmt.Fprintf(w, "cdcs_raft_is_leader{raft_node=%q} %d\n", id, isLeader)
	}

	fmt.Fprintln(w, "# HELP cdcs_raft_term Current Raft term reported by each active node")
	fmt.Fprintln(w, "# TYPE cdcs_raft_term gauge")

	for id, entry := range raftLive {
		term, _ := entry["term"].(float64)
		fmt.Fprintf(w, "cdcs_raft_term{raft_node=%q} %.0f\n", id, term)
	}

	fmt.Fprintln(w, "# HELP cdcs_raft_commit_index Current Raft commit index reported by each active node")
	fmt.Fprintln(w, "# TYPE cdcs_raft_commit_index gauge")

	for id, entry := range raftLive {
		commitIndex, _ := entry["commit_index"].(float64)
		fmt.Fprintf(w, "cdcs_raft_commit_index{raft_node=%q} %.0f\n", id, commitIndex)
	}

	// ============================================================
	// FILE / RS
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_files_stored Number of files currently tracked")
	fmt.Fprintln(w, "# TYPE cdcs_files_stored gauge")
	fmt.Fprintf(w, "cdcs_files_stored %d\n", fileCount)

	fmt.Fprintln(w, "# HELP cdcs_original_bytes Total original file bytes tracked")
	fmt.Fprintln(w, "# TYPE cdcs_original_bytes gauge")
	fmt.Fprintf(w, "cdcs_original_bytes %d\n", originalBytes)

	fmt.Fprintln(w, "# HELP cdcs_shards_stored Total Reed-Solomon shards currently mapped across all files")
	fmt.Fprintln(w, "# TYPE cdcs_shards_stored gauge")
	fmt.Fprintf(w, "cdcs_shards_stored %d\n", totalShards)

	fmt.Fprintln(w, "# HELP cdcs_rs_data_shards Configured Reed-Solomon data shard count")
	fmt.Fprintln(w, "# TYPE cdcs_rs_data_shards gauge")
	fmt.Fprintf(w, "cdcs_rs_data_shards %d\n", rsDataShards)

	fmt.Fprintln(w, "# HELP cdcs_rs_parity_shards Configured Reed-Solomon parity shard count")
	fmt.Fprintln(w, "# TYPE cdcs_rs_parity_shards gauge")
	fmt.Fprintf(w, "cdcs_rs_parity_shards %d\n", rsParityShards)

	fmt.Fprintln(w, "# HELP cdcs_rs_total_shards Total Reed-Solomon shards per file")
	fmt.Fprintln(w, "# TYPE cdcs_rs_total_shards gauge")
	fmt.Fprintf(w, "cdcs_rs_total_shards %d\n", rsTotalShards)

	// ============================================================
	// SHARD FETCH
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_shard_fetch_success_total Individual shard fetch attempts that succeeded")
	fmt.Fprintln(w, "# TYPE cdcs_shard_fetch_success_total counter")
	fmt.Fprintf(
		w,
		"cdcs_shard_fetch_success_total %d\n",
		atomic.LoadInt64(&b.shardFetchSuccessTotal),
	)

	fmt.Fprintln(w, "# HELP cdcs_shard_fetch_fail_total Individual shard fetch attempts that failed")
	fmt.Fprintln(w, "# TYPE cdcs_shard_fetch_fail_total counter")
	fmt.Fprintf(
		w,
		"cdcs_shard_fetch_fail_total %d\n",
		atomic.LoadInt64(&b.shardFetchFailTotal),
	)

	fmt.Fprintln(w, "# HELP cdcs_shard_fetch_duration_ms Shard fetch latency")
	fmt.Fprintln(w, "# TYPE cdcs_shard_fetch_duration_ms histogram")
	shardFetchLatencyMs.WriteProm(w, "cdcs_shard_fetch_duration_ms")

	// ============================================================
	// RECONSTRUCTION
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_reconstruct_total Downloads that actually required Reed-Solomon data reconstruction")
	fmt.Fprintln(w, "# TYPE cdcs_reconstruct_total counter")
	fmt.Fprintf(
		w,
		"cdcs_reconstruct_total %d\n",
		atomic.LoadInt64(&b.reconstructTotal),
	)

	fmt.Fprintln(w, "# HELP cdcs_reconstruction_tests_total Reconstruction fault-tolerance tests")
	fmt.Fprintln(w, "# TYPE cdcs_reconstruction_tests_total counter")
	fmt.Fprintf(w, "cdcs_reconstruction_tests_total %d\n", atomic.LoadInt64(&reconstructionTestsTotal))

	fmt.Fprintln(w, "# HELP cdcs_reconstruction_success_total Successful reconstruction fault-tolerance tests")
	fmt.Fprintln(w, "# TYPE cdcs_reconstruction_success_total counter")
	fmt.Fprintf(w, "cdcs_reconstruction_success_total %d\n", atomic.LoadInt64(&reconstructionSuccessTotal))

	fmt.Fprintln(w, "# HELP cdcs_reconstruction_failure_total Failed reconstruction fault-tolerance tests")
	fmt.Fprintln(w, "# TYPE cdcs_reconstruction_failure_total counter")
	fmt.Fprintf(w, "cdcs_reconstruction_failure_total %d\n", atomic.LoadInt64(&reconstructionFailureTotal))

	// ============================================================
	// UPLOAD / DOWNLOAD
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_upload_requests_total Total /upload requests received")
	fmt.Fprintln(w, "# TYPE cdcs_upload_requests_total counter")
	fmt.Fprintf(w, "cdcs_upload_requests_total %d\n", atomic.LoadInt64(&b.uploadRequestsTotal))

	fmt.Fprintln(w, "# HELP cdcs_upload_success_total Total /upload requests that completed successfully")
	fmt.Fprintln(w, "# TYPE cdcs_upload_success_total counter")
	fmt.Fprintf(w, "cdcs_upload_success_total %d\n", atomic.LoadInt64(&b.uploadSuccessTotal))

	fmt.Fprintln(w, "# HELP cdcs_download_requests_total Total /download requests received")
	fmt.Fprintln(w, "# TYPE cdcs_download_requests_total counter")
	fmt.Fprintf(w, "cdcs_download_requests_total %d\n", atomic.LoadInt64(&b.sfTotal))

	fmt.Fprintln(w, "# HELP cdcs_download_success_total Total /download requests that completed successfully")
	fmt.Fprintln(w, "# TYPE cdcs_download_success_total counter")
	fmt.Fprintf(w, "cdcs_download_success_total %d\n", atomic.LoadInt64(&b.downloadSuccessTotal))

	fmt.Fprintln(w, "# HELP cdcs_download_duration_ms Download request latency in milliseconds")
	fmt.Fprintln(w, "# TYPE cdcs_download_duration_ms histogram")
	b.downloadLatencyMs.WriteProm(w, "cdcs_download_duration_ms")

	fmt.Fprintln(w, "# HELP cdcs_upload_duration_ms Upload request latency in milliseconds")
	fmt.Fprintln(w, "# TYPE cdcs_upload_duration_ms histogram")
	b.uploadLatencyMs.WriteProm(w, "cdcs_upload_duration_ms")

	// ============================================================
	// CACHE (summed across every storage node's own 2Q engine)
	// ============================================================

	var memHits, diskHits, missCount, totalStorageBytes int64

	aggHitLatency := NewHistogram(cacheHitLatencyMs.buckets)
	aggMissLatency := NewHistogram(cacheMissLatencyMs.buckets)

	for _, s := range nodeStats {
		memHits += s.MemHits
		diskHits += s.DiskHits
		missCount += s.Misses
		totalStorageBytes += s.StorageBytes

		// Skip a node whose histogram doesn't match our bucket layout
		// (e.g. still running an older build without these fields).
		if len(s.HitLatencyCounts) == len(aggHitLatency.counts) {
			for i, c := range s.HitLatencyCounts {
				aggHitLatency.counts[i] += c
			}
			aggHitLatency.sum += s.HitLatencySum
			aggHitLatency.count += s.HitLatencyCount
		}

		if len(s.MissLatencyCounts) == len(aggMissLatency.counts) {
			for i, c := range s.MissLatencyCounts {
				aggMissLatency.counts[i] += c
			}
			aggMissLatency.sum += s.MissLatencySum
			aggMissLatency.count += s.MissLatencyCount
		}
	}

	cacheMisses := diskHits + missCount
	cacheTotal := memHits + cacheMisses

	hitRatio := 0.0
	if cacheTotal > 0 {
		hitRatio = float64(memHits) / float64(cacheTotal)
	}

	fmt.Fprintln(w, "# HELP cdcs_cache_hits_total Shard reads served from RAM (2Q cache hit)")
	fmt.Fprintln(w, "# TYPE cdcs_cache_hits_total counter")
	fmt.Fprintf(w, "cdcs_cache_hits_total %d\n", memHits)

	fmt.Fprintln(w, "# HELP cdcs_cache_misses_total Shard reads that missed the RAM cache (served from disk or not found)")
	fmt.Fprintln(w, "# TYPE cdcs_cache_misses_total counter")
	fmt.Fprintf(w, "cdcs_cache_misses_total %d\n", cacheMisses)

	fmt.Fprintln(w, "# HELP cdcs_cache_hit_ratio Cumulative 2Q cache hit ratio")
	fmt.Fprintln(w, "# TYPE cdcs_cache_hit_ratio gauge")
	fmt.Fprintf(w, "cdcs_cache_hit_ratio %f\n", hitRatio)

	fmt.Fprintln(w, "# HELP cdcs_strict_cache_hit_ratio Strict RAM-only 2Q cache hit ratio")
	fmt.Fprintln(w, "# TYPE cdcs_strict_cache_hit_ratio gauge")
	fmt.Fprintf(w, "cdcs_strict_cache_hit_ratio %f\n", hitRatio)

	fmt.Fprintln(w, "# HELP cdcs_cache_hit_duration_ms Cache hit latency")
	fmt.Fprintln(w, "# TYPE cdcs_cache_hit_duration_ms histogram")
	aggHitLatency.WriteProm(w, "cdcs_cache_hit_duration_ms")

	fmt.Fprintln(w, "# HELP cdcs_cache_miss_duration_ms Cache miss latency")
	fmt.Fprintln(w, "# TYPE cdcs_cache_miss_duration_ms histogram")
	aggMissLatency.WriteProm(w, "cdcs_cache_miss_duration_ms")

	// ============================================================
	// SINGLEFLIGHT / STAMPEDE
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_deduped_requests_total Download requests that shared an in-flight singleflight call")
	fmt.Fprintln(w, "# TYPE cdcs_deduped_requests_total counter")
	fmt.Fprintf(w, "cdcs_deduped_requests_total %d\n", atomic.LoadInt64(&b.sfDeduped))

	fmt.Fprintln(w, "# HELP cdcs_singleflight_inflight Current number of in-flight download calls")
	fmt.Fprintln(w, "# TYPE cdcs_singleflight_inflight gauge")
	fmt.Fprintf(w, "cdcs_singleflight_inflight %d\n", atomic.LoadInt64(&b.sfInflight))

	fmt.Fprintln(w, "# HELP cdcs_stampede_requests_total Requests recorded by stampede tests")
	fmt.Fprintln(w, "# TYPE cdcs_stampede_requests_total counter")
	fmt.Fprintf(w, "cdcs_stampede_requests_total %d\n", atomic.LoadInt64(&stampedeRequestsTotal))

	fmt.Fprintln(w, "# HELP cdcs_stampede_storage_fetches_total Actual storage fetches recorded by stampede tests")
	fmt.Fprintln(w, "# TYPE cdcs_stampede_storage_fetches_total counter")
	fmt.Fprintf(w, "cdcs_stampede_storage_fetches_total %d\n", atomic.LoadInt64(&stampedeStorageFetchesTotal))

	// ============================================================
	// NODE FAILURE / HEALING
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_kill_events_total Times a node was killed via the dashboard")
	fmt.Fprintln(w, "# TYPE cdcs_kill_events_total counter")
	fmt.Fprintf(w, "cdcs_kill_events_total %d\n", atomic.LoadInt64(&b.killEventsTotal))

	fmt.Fprintln(w, "# HELP cdcs_revive_events_total Times a node was revived via the dashboard")
	fmt.Fprintln(w, "# TYPE cdcs_revive_events_total counter")
	fmt.Fprintf(w, "cdcs_revive_events_total %d\n", atomic.LoadInt64(&b.reviveEventsTotal))

	fmt.Fprintln(w, "# HELP cdcs_heal_duration_ms Time required to restore a node and its shards")
	fmt.Fprintln(w, "# TYPE cdcs_heal_duration_ms histogram")
	healLatencyMs.WriteProm(w, "cdcs_heal_duration_ms")

	fmt.Fprintln(w, "# HELP cdcs_sweeper_evictions_total Files removed by the storage sweeper")
	fmt.Fprintln(w, "# TYPE cdcs_sweeper_evictions_total counter")
	fmt.Fprintf(w, "cdcs_sweeper_evictions_total %d\n", atomic.LoadInt64(&b.sweeperEvictionsTotal))

	// ============================================================
	// RAFT LEADER ELECTION
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_leader_election_duration_ms Time from node kill until a new Raft leader is detected")
	fmt.Fprintln(w, "# TYPE cdcs_leader_election_duration_ms histogram")
	leaderElectionLatencyMs.WriteProm(w, "cdcs_leader_election_duration_ms")

	// ============================================================
	// CORRECTNESS TEST COUNTERS
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_metadata_consistency_checks_total Metadata consistency checks performed")
	fmt.Fprintln(w, "# TYPE cdcs_metadata_consistency_checks_total counter")
	fmt.Fprintf(w, "cdcs_metadata_consistency_checks_total %d\n", atomic.LoadInt64(&metadataConsistencyChecksTotal))

	fmt.Fprintln(w, "# HELP cdcs_metadata_consistency_pass_total Metadata consistency checks that passed")
	fmt.Fprintln(w, "# TYPE cdcs_metadata_consistency_pass_total counter")
	fmt.Fprintf(w, "cdcs_metadata_consistency_pass_total %d\n", atomic.LoadInt64(&metadataConsistencyPassTotal))

	fmt.Fprintln(w, "# HELP cdcs_metadata_consistency_fail_total Metadata consistency checks that failed")
	fmt.Fprintln(w, "# TYPE cdcs_metadata_consistency_fail_total counter")
	fmt.Fprintf(w, "cdcs_metadata_consistency_fail_total %d\n", atomic.LoadInt64(&metadataConsistencyFailTotal))

	fmt.Fprintln(w, "# HELP cdcs_byte_match_checks_total Byte-for-byte correctness checks performed")
	fmt.Fprintln(w, "# TYPE cdcs_byte_match_checks_total counter")
	fmt.Fprintf(w, "cdcs_byte_match_checks_total %d\n", atomic.LoadInt64(&byteMatchChecksTotal))

	fmt.Fprintln(w, "# HELP cdcs_byte_match_pass_total Byte-for-byte correctness checks that passed")
	fmt.Fprintln(w, "# TYPE cdcs_byte_match_pass_total counter")
	fmt.Fprintf(w, "cdcs_byte_match_pass_total %d\n", atomic.LoadInt64(&byteMatchPassTotal))

	fmt.Fprintln(w, "# HELP cdcs_byte_match_fail_total Byte-for-byte correctness checks that failed")
	fmt.Fprintln(w, "# TYPE cdcs_byte_match_fail_total counter")
	fmt.Fprintf(w, "cdcs_byte_match_fail_total %d\n", atomic.LoadInt64(&byteMatchFailTotal))

	fmt.Fprintln(w, "# HELP cdcs_eviction_checks_total Cache eviction correctness checks performed")
	fmt.Fprintln(w, "# TYPE cdcs_eviction_checks_total counter")
	fmt.Fprintf(w, "cdcs_eviction_checks_total %d\n", atomic.LoadInt64(&evictionChecksTotal))

	fmt.Fprintln(w, "# HELP cdcs_eviction_pass_total Cache eviction correctness checks that passed")
	fmt.Fprintln(w, "# TYPE cdcs_eviction_pass_total counter")
	fmt.Fprintf(w, "cdcs_eviction_pass_total %d\n", atomic.LoadInt64(&evictionPassTotal))

	fmt.Fprintln(w, "# HELP cdcs_eviction_fail_total Cache eviction correctness checks that failed")
	fmt.Fprintln(w, "# TYPE cdcs_eviction_fail_total counter")
	fmt.Fprintf(w, "cdcs_eviction_fail_total %d\n", atomic.LoadInt64(&evictionFailTotal))

	// ============================================================
	// RESOURCE USAGE (orchestrator process only)
	// ============================================================

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Fprintln(w, "# HELP cdcs_process_memory_bytes Current Go process memory allocation")
	fmt.Fprintln(w, "# TYPE cdcs_process_memory_bytes gauge")
	fmt.Fprintf(w, "cdcs_process_memory_bytes %d\n", mem.Alloc)

	fmt.Fprintln(w, "# HELP cdcs_process_heap_bytes Current Go heap allocation")
	fmt.Fprintln(w, "# TYPE cdcs_process_heap_bytes gauge")
	fmt.Fprintf(w, "cdcs_process_heap_bytes %d\n", mem.HeapAlloc)

	fmt.Fprintln(w, "# HELP cdcs_process_goroutines Current number of goroutines")
	fmt.Fprintln(w, "# TYPE cdcs_process_goroutines gauge")
	fmt.Fprintf(w, "cdcs_process_goroutines %d\n", runtime.NumGoroutine())

	if p, err := process.NewProcess(int32(os.Getpid())); err == nil {
		if cpu, err := p.CPUPercent(); err == nil {
			fmt.Fprintln(w, "# HELP cdcs_process_cpu_percent Current Go process CPU utilization")
			fmt.Fprintln(w, "# TYPE cdcs_process_cpu_percent gauge")
			fmt.Fprintf(w, "cdcs_process_cpu_percent %f\n", cpu)
		}
	}

	// ============================================================
	// STORAGE (each node reports the size of its own storage dir)
	// ============================================================

	fmt.Fprintln(w, "# HELP cdcs_storage_bytes Actual bytes occupied by all node storage")
	fmt.Fprintln(w, "# TYPE cdcs_storage_bytes gauge")
	fmt.Fprintf(w, "cdcs_storage_bytes %d\n", totalStorageBytes)

	fmt.Fprintln(w, "# HELP cdcs_node_storage_bytes Actual bytes occupied by one node's storage")
	fmt.Fprintln(w, "# TYPE cdcs_node_storage_bytes gauge")

	for id, s := range nodeStats {
		fmt.Fprintf(
			w,
			"cdcs_node_storage_bytes{node=%q,backup=%q} %d\n",
			id,
			boolStr(backupSet[id]),
			s.StorageBytes,
		)
	}

	fmt.Fprintln(w, "# HELP cdcs_storage_overhead_ratio Physical storage divided by original file bytes")
	fmt.Fprintln(w, "# TYPE cdcs_storage_overhead_ratio gauge")

	if originalBytes > 0 {
		fmt.Fprintf(
			w,
			"cdcs_storage_overhead_ratio %f\n",
			float64(totalStorageBytes)/float64(originalBytes),
		)
	} else {
		fmt.Fprintln(w, "cdcs_storage_overhead_ratio 0")
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}

	return "false"
}