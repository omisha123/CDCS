package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "cdcs1/cdcspb"
)

// StorageNode represents a remote physical PC in your storage mesh.
// It is intentionally separated from server.go to exclusively manage the internal ReplicationService[cite: 4].
type StorageNode struct {
	ID      string
	Address string // e.g., 192.168.1.201:50051
	Client  pb.ReplicationServiceClient
	Conn    *grpc.ClientConn
	IsAlive bool
}

// NewStorageNode establishes a persistent gRPC connection from the laptop to a physical Storage PC.
func NewStorageNode(id, address string) (*StorageNode, error) {
	// Dial the physical IP address without hardcoding localhost
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s at %s: %w", id, address, err)
	}

	client := pb.NewReplicationServiceClient(conn)

	return &StorageNode{
		ID:      id,
		Address: address,
		Client:  client,
		Conn:    conn,
		IsAlive: true,
	}, nil
}

// PushShard sends a Reed-Solomon chunk over the network to be written on the physical PC disk[cite: 4].
func (n *StorageNode) PushShard(ctx context.Context, fileID string, shardIdx int, data []byte) error {
	req := &pb.ShardRequest{
		FileId:     fileID,
		ShardIndex: int32(shardIdx),
		Data:       data,
	}
	
	_, err := n.Client.PutShard(ctx, req)
	return err
}

// FetchShard retrieves a chunk from the physical PC during adaptive fetch racing or reconstruction[cite: 4].
func (n *StorageNode) FetchShard(ctx context.Context, fileID string, shardIdx int) ([]byte, error) {
	req := &pb.ShardRequest{
		FileId:     fileID,
		ShardIndex: int32(shardIdx),
	}

	resp, err := n.Client.GetShard(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// WipeShard physically deletes the chunk during background sweeping or LRU eviction[cite: 4].
func (n *StorageNode) WipeShard(ctx context.Context, key string, shardIdx int) error {
	req := &pb.DeleteRequest{
		Key:        key,
		ShardIndex: int32(shardIdx),
	}
	
	_, err := n.Client.DeleteShard(ctx, req)
	return err
}

// CheckHealth pings the gRPC connection state to detect if a PC was physically unplugged during chaos testing.
func (n *StorageNode) CheckHealth() {
	state := n.Conn.GetState()
	// If the state is TransientFailure (14) or Shutdown (4), the physical node is offline
	if state.String() == "TRANSIENT_FAILURE" || state.String() == "SHUTDOWN" {
		n.IsAlive = false
	} else {
		n.IsAlive = true
	}
}

// Close cleanly shuts down the network connection when tearing down the cluster.
func (n *StorageNode) Close() {
	if n.Conn != nil {
		n.Conn.Close()
	}
}