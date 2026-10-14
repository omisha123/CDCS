package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "cdcs1/cdcspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ClusterClient is the single place gRPC connections to other nodes are
// made and used. backend.go talks to the cluster only through this type -
// it no longer dials/calls gRPC itself.
type ClusterClient struct {
	cacheClients map[string]pb.CacheServiceClient
	replClients  map[string]pb.ReplicationServiceClient
}

// maxShardMsgSize sets the gRPC message size ceiling for shard traffic.
// gRPC's built-in default is 4MB in both directions; any shard bigger than
// that would fail PutShard against the main nodes with ResourceExhausted
// and silently fall back to the two backup-only nodes for every shard.
// Set in GB so it's easy to size against your largest expected file.
const maxShardMsgSizeGB = 1
const maxShardMsgSize = maxShardMsgSizeGB << 30 // GB -> bytes

// pingTimeout controls how long PingNode waits before calling a node
// unreachable. Configurable via PING_TIMEOUT_MS (milliseconds) so it can be
// loosened on networks with real jitter without a code change; falls back
// to 200ms, the original hardcoded value, if unset or invalid.
var pingTimeout = func() time.Duration {
	if v := os.Getenv("PING_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 200 * time.Millisecond
}()

func NewClusterClient(nodes map[string]string) *ClusterClient {
	// When CDCS_ADMIN_TOKEN is set, every outgoing call carries it so the
	// storage nodes (which check it) accept this orchestrator.
	token := os.Getenv("CDCS_ADMIN_TOKEN")
	addToken := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-admin-token", token)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	cacheClients := make(map[string]pb.CacheServiceClient)
	replClients := make(map[string]pb.ReplicationServiceClient)
	for id, addr := range nodes {
		conn, err := grpc.Dial(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(addToken),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallSendMsgSize(maxShardMsgSize),
				grpc.MaxCallRecvMsgSize(maxShardMsgSize),
			),
		)
		if err == nil {
			cacheClients[id] = pb.NewCacheServiceClient(conn)
			replClients[id] = pb.NewReplicationServiceClient(conn)
		}
	}
	return &ClusterClient{cacheClients: cacheClients, replClients: replClients}
}

// PingNode does a fast health check against a node using its CacheService.
func (c *ClusterClient) PingNode(nodeID string) bool {
	client, exists := c.cacheClients[nodeID]
	if !exists {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	_, err := client.Get(ctx, &pb.GetRequest{Key: "__health_check__"})
	return err == nil
}

// PutShard pushes one Reed-Solomon shard to nodeID via ReplicationService.
// checksum is not part of the wire message in this proto - it's still
// accepted here (backend.go passes it) but only used by the caller for
// its own checksumMap bookkeeping; nothing is sent or verified over gRPC.
func (c *ClusterClient) PutShard(ctx context.Context, nodeID, fileID string, shardIndex uint32, shardData []byte, checksum string) error {
	client, exists := c.replClients[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	res, err := client.PutShard(ctx, &pb.ShardRequest{
		FileId:     fileID,
		ShardIndex: int32(shardIndex),
		Data:       shardData,
	})
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("node %s rejected shard write", nodeID)
	}
	return nil
}

// GetShard fetches one Reed-Solomon shard from nodeID via ReplicationService.
// The returned checksum is computed locally from the received bytes (this
// proto doesn't carry one on the wire) so callers can still compare it
// against the checksum recorded at upload time.
func (c *ClusterClient) GetShard(ctx context.Context, nodeID, fileID string, shardIndex uint32) ([]byte, string, error) {
	client, exists := c.replClients[nodeID]
	if !exists {
		return nil, "", fmt.Errorf("node %s not found", nodeID)
	}

	res, err := client.GetShard(ctx, &pb.ShardRequest{
		FileId:     fileID,
		ShardIndex: int32(shardIndex),
	})
	if err != nil {
		return nil, "", err
	}
	if !res.Success {
		return nil, "", fmt.Errorf("shard unavailable on node %s", nodeID)
	}
	return res.Data, shardChecksum(res.Data), nil
}

// DeleteShard removes one shard from nodeID via ReplicationService, so it
// clears the same CacheEngine entry PutShard/GetShard use.
func (c *ClusterClient) DeleteShard(ctx context.Context, nodeID, fileID string, shardIndex uint32) error {
	client, exists := c.replClients[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	res, err := client.DeleteShard(ctx, &pb.DeleteRequest{
		Key:        fileID,
		ShardIndex: int32(shardIndex),
	})
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("node %s rejected shard delete", nodeID)
	}
	return nil
}