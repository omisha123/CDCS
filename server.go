package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"

	pb "cdcs1/cdcspb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// shardIDPattern is the only shape of file ID a storage node will accept.
// The orchestrator already sanitizes IDs on upload, but the gRPC port is
// reachable directly, so the node has to check too (a FileId like
// "../../x" would otherwise write outside storageDir).
var shardIDPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,200}$`)

func validShardRequest(fileID string, shardIndex int32) error {
	if !shardIDPattern.MatchString(fileID) || shardIndex < 0 {
		return status.Error(codes.InvalidArgument, "invalid file id or shard index")
	}
	return nil
}

// Server defines the core gRPC server object for your nodes.
type Server struct {
	pb.UnimplementedCacheServiceServer
	pb.UnimplementedReplicationServiceServer

	engine     *TwoQEngine
	nodeID     string
	storageDir string

	memHits  int64
	diskHits int64
	misses   int64
}

func NewServer(nodeID, storageDir string, engine *TwoQEngine) *Server {
	return &Server{
		nodeID:     nodeID,
		storageDir: storageDir,
		engine:     engine,
	}
}

// =====================================================================
// CACHE SERVICE
// =====================================================================

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	data, found := s.engine.Get(req.Key)
	return &pb.GetResponse{Data: data, Success: found}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.engine.Put(req.Key, req.Data)
	return &pb.PutResponse{Success: true}, nil
}

// Delete now actually removes the key from the 2Q cache.
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.engine.Delete(req.Key)
	return &pb.DeleteResponse{Success: true}, nil
}

func (s *Server) InspectCache(ctx context.Context, req *pb.InspectRequest) (*pb.InspectResponse, error) {
	contents := s.engine.Inspect()

	return &pb.InspectResponse{
		A1In:  contents.A1in,
		Am:    contents.Am,
		A1Out: contents.A1out,
	}, nil
}

// =====================================================================
// REPLICATION SERVICE
// =====================================================================

func (s *Server) PutShard(ctx context.Context, req *pb.ShardRequest) (*pb.ShardResponse, error) {
	if err := validShardRequest(req.FileId, req.ShardIndex); err != nil {
		return &pb.ShardResponse{Success: false}, err
	}
	chunkName := fmt.Sprintf("%s_shard_%d", req.FileId, req.ShardIndex)
	path := filepath.Join(s.storageDir, chunkName)

	if err := os.WriteFile(path, req.Data, 0644); err != nil {
		return &pb.ShardResponse{Success: false}, err
	}

	s.engine.Put(chunkName, req.Data)
	return &pb.ShardResponse{Success: true}, nil
}

func (s *Server) GetShard(ctx context.Context, req *pb.ShardRequest) (*pb.ShardResponse, error) {
	if err := validShardRequest(req.FileId, req.ShardIndex); err != nil {
		return &pb.ShardResponse{Success: false}, err
	}
	chunkName := fmt.Sprintf("%s_shard_%d", req.FileId, req.ShardIndex)

	// 1. RAM hit
	start := time.Now()
	if data, found := s.engine.Get(chunkName); found {
		atomic.AddInt64(&s.memHits, 1)
		ObserveCacheHitLatency(float64(time.Since(start).Microseconds()) / 1000.0)
		return &pb.ShardResponse{Data: data, Success: true}, nil
	}

	// 2. RAM miss, check disk
	path := filepath.Join(s.storageDir, chunkName)
	data, err := os.ReadFile(path)
	if err != nil {
		// Missing from both RAM and disk: a true miss.
		atomic.AddInt64(&s.misses, 1)
		ObserveCacheMissLatency(float64(time.Since(start).Microseconds()) / 1000.0)
		return &pb.ShardResponse{Success: false}, nil
	}

	// Found on disk: this is a disk hit (it was a RAM miss, but not a true miss).
	atomic.AddInt64(&s.diskHits, 1)

	// Pull into memory for future requests
	s.engine.Put(chunkName, data)
	ObserveCacheMissLatency(float64(time.Since(start).Microseconds()) / 1000.0)
	return &pb.ShardResponse{Data: data, Success: true}, nil
}

func (s *Server) DeleteShard(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validShardRequest(req.Key, req.ShardIndex); err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}
	chunkName := fmt.Sprintf("%s_shard_%d", req.Key, req.ShardIndex)
	path := filepath.Join(s.storageDir, chunkName)

	_ = os.Remove(path)
	// Also drop it from RAM so a deleted shard can't be served from cache.
	s.engine.Delete(chunkName)

	return &pb.DeleteResponse{Success: true}, nil
}

func (s *Server) GetCacheStats(ctx context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	cacheHitLatencyMs.mu.Lock()
	hitCounts := append([]int64(nil), cacheHitLatencyMs.counts...)
	hitSum, hitCount := cacheHitLatencyMs.sum, cacheHitLatencyMs.count
	cacheHitLatencyMs.mu.Unlock()

	cacheMissLatencyMs.mu.Lock()
	missCounts := append([]int64(nil), cacheMissLatencyMs.counts...)
	missSum, missCount := cacheMissLatencyMs.sum, cacheMissLatencyMs.count
	cacheMissLatencyMs.mu.Unlock()

	return &pb.StatsResponse{
		MemHits:           atomic.LoadInt64(&s.memHits),
		DiskHits:          atomic.LoadInt64(&s.diskHits),
		Misses:            atomic.LoadInt64(&s.misses),
		StorageBytes:      cachedDirectorySize(s.storageDir),
		HitLatencyCounts:  hitCounts,
		HitLatencySum:     hitSum,
		HitLatencyCount:   hitCount,
		MissLatencyCounts: missCounts,
		MissLatencySum:    missSum,
		MissLatencyCount:  missCount,
	}, nil
}