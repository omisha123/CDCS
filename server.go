package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	pb "cdcs1/cdcspb" 
)

// Server defines the core gRPC server object for your nodes[cite: 5].
// By embedding the Unimplemented structs, it automatically satisfies the gRPC interface requirements[cite: 5].
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
// NewServer initializes the Server object[cite: 5].
// It accepts nodeID and storageDir dynamically to completely eliminate local hardcoding for the 15-PC deployment.
func NewServer(nodeID, storageDir string, engine *TwoQEngine) *Server {
	return &Server{
		nodeID:     nodeID,
		storageDir: storageDir,
		engine:     engine,
	}
}

// =====================================================================
// CACHE SERVICE (Client-Facing API Gateway)
// =====================================================================

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	data, found := s.engine.Get(req.Key)
	return &pb.GetResponse{Data: data, Success: found}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.engine.Put(req.Key, req.Data)
	return &pb.PutResponse{Success: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{Success: true}, nil
}

// InspectCache allows the remote orchestrator laptop to view this physical PC's 2Q RAM queues over the LAN.
func (s *Server) InspectCache(ctx context.Context, req *pb.InspectRequest) (*pb.InspectResponse, error) {
	contents := s.engine.Inspect()
	
	return &pb.InspectResponse{
		A1In:  contents.A1in,
		Am:    contents.Am,
		A1Out: contents.A1out,
	}, nil
}

// =====================================================================
// REPLICATION SERVICE (Internal Storage Mesh for Reed-Solomon Shards)
// =====================================================================

// PutShard stores an incoming Reed-Solomon chunk onto this PC's physical disk.
func (s *Server) PutShard(ctx context.Context, req *pb.ShardRequest) (*pb.ShardResponse, error) {
	chunkName := fmt.Sprintf("%s_shard_%d", req.FileId, req.ShardIndex)
	path := filepath.Join(s.storageDir, chunkName)
	
	// Write directly to the physical hardware path mounted via Docker
	if err := os.WriteFile(path, req.Data, 0644); err != nil {
		return &pb.ShardResponse{Success: false}, err
	}
	
	// Push into the 2Q RAM cache for immediate hot reads
	s.engine.Put(chunkName, req.Data)
	return &pb.ShardResponse{Success: true}, nil
}

// GetShard retrieves a Reed-Solomon chunk for the orchestrator during a download or failover.
func (s *Server) GetShard(ctx context.Context, req *pb.ShardRequest) (*pb.ShardResponse, error) {
	chunkName := fmt.Sprintf("%s_shard_%d", req.FileId, req.ShardIndex)
	
	// 1. Try RAM First (Memory Hit)
	if data, found := s.engine.Get(chunkName); found {
		atomic.AddInt64(&s.memHits, 1)
		return &pb.ShardResponse{Data: data, Success: true}, nil
	}
	
	// 2. It missed the RAM cache. Let's check the physical disk.
	path := filepath.Join(s.storageDir, chunkName)
	data, err := os.ReadFile(path)
	if err != nil {
		// Truly missing from both RAM and Disk
		atomic.AddInt64(&s.misses, 1)
		return &pb.ShardResponse{Success: false}, nil 
	}
	
	// It was found on disk, but it was a RAM cache miss!
	atomic.AddInt64(&s.misses, 1) // <-- Increment cache miss here
	
	// Pull into memory for future requests
	s.engine.Put(chunkName, data)
	return &pb.ShardResponse{Data: data, Success: true}, nil
}

// DeleteShard is a primary gRPC handler responsible for physically removing data during garbage collection[cite: 5].
func (s *Server) DeleteShard(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	chunkName := fmt.Sprintf("%s_shard_%d", req.Key, req.ShardIndex)
	path := filepath.Join(s.storageDir, chunkName)
	
	// Execute os.Remove() to physically wipe the file chunk from the local disk[cite: 5].
	_ = os.Remove(path) 
	
	return &pb.DeleteResponse{Success: true}, nil
}

func (s *Server) GetCacheStats(ctx context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	return &pb.StatsResponse{
		MemHits:  atomic.LoadInt64(&s.memHits),
		DiskHits: atomic.LoadInt64(&s.diskHits),
		Misses:   atomic.LoadInt64(&s.misses),
	}, nil
}