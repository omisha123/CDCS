package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "cdcs1/cdcspb"
	"google.golang.org/grpc"
)

func main() {
	// Support both Docker environment variables and CLI flags.
	roleEnv := os.Getenv("ROLE")
	nodeIDEnv := os.Getenv("NODE_ID")
	bindAddrEnv := os.Getenv("BIND_IP")
	advertiseAddrEnv := os.Getenv("ADVERTISE_ADDR")
	storageDirEnv := os.Getenv("STORAGE_DIR")
	raftPeersEnv := os.Getenv("RAFT_PEERS")
	raftSeedEnv := os.Getenv("RAFT_SEED")

	roleFlag := flag.String("role", roleEnv, "Role of the node: 'orchestrator', 'storage', or 'raft'")
	nodeIDFlag := flag.String("id", nodeIDEnv, "Unique ID (e.g., Node-01, Raft-01)")
	bindAddrFlag := flag.String("bind", bindAddrEnv, "Physical LAN IP and Port to bind to")
	advertiseAddrFlag := flag.String("advertise", advertiseAddrEnv, "Address other Raft peers should dial")
	storageDirFlag := flag.String("storage", storageDirEnv, "Path to physical storage directory")
	configFlag := flag.String("config", "cluster.json", "Path to cluster configuration file for orchestrator")
	raftPeersFlag := flag.String("raft-peers", raftPeersEnv, "Comma-separated NodeID=advertiseAddr list of the 5 active Raft nodes. Leave empty for a Raft standby node.")
	raftSeedFlag := flag.String("raft-seed", raftSeedEnv, "Raft standby only: raft advertise address of any active Raft node to join through, e.g. raft-01:60051")

	flag.Parse()

	if *storageDirFlag == "" {
		*storageDirFlag = "/app/storage"
	}

	if *roleFlag == "" {
		log.Fatal("Fatal: ROLE is required (orchestrator, storage, or raft)")
	}

	switch *roleFlag {
	case "orchestrator":
		startOrchestrator(*configFlag)

	case "storage":
		startStorageNode(*nodeIDFlag, *bindAddrFlag, *storageDirFlag)

	case "raft":
		startRaftNode(*nodeIDFlag, *bindAddrFlag, *advertiseAddrFlag, parseRaftPeers(*raftPeersFlag), *raftSeedFlag)

	default:
		log.Fatalf("Unknown role: %s", *roleFlag)
	}
}

// startOrchestrator runs the CDCS API gateway and passes the cluster.json
// config path to the backend.
func startOrchestrator(configPath string) {
	fmt.Printf("[Orchestrator] Starting CDCS API Gateway on port 8080 using %s...\n", configPath)

	backend := NewBackend(configPath)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.HandleFunc("/upload", backend.HandleUpload)
	http.HandleFunc("/download", backend.HandleDownload)
	http.HandleFunc("/stats", backend.HandleStats)
	http.HandleFunc("/nodes", backend.HandleNodes)
	http.HandleFunc("/cache-inspect", backend.HandleCacheInspect)
	http.HandleFunc("/raft/status", backend.HandleRaftStatus)
	http.HandleFunc("/raft/promote", backend.HandlePromoteRaft)
	http.HandleFunc("/files", backend.HandleFiles)
	http.HandleFunc("/view", backend.HandleView)
	http.HandleFunc("/delete", backend.HandleDelete)
	http.HandleFunc("/pin", backend.HandleTogglePin)
	http.HandleFunc("/cache-stats", backend.HandleCacheStats)
	http.HandleFunc("/kill-node", backend.HandleKillNode)
	http.HandleFunc("/revive-node", backend.HandleReviveNode)
	http.HandleFunc("/events", backend.HandleEvents)
	http.HandleFunc("/metrics", backend.HandleMetrics)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func startStorageNode(nodeID, bindAddr, storageDir string) {
	fmt.Printf("[%s] Starting Storage Node on %s\n", nodeID, bindAddr)

	// Was NewTwoQEngine(10): capacity 10 means a1inCap = 10/4 = 2, so the
	// 3rd distinct key written immediately evicted the 1st into the ghost
	// queue (a1out) - nearly every shard ended up "ghosted" instead of
	// actually cached. 500 gives the cache room to hold a realistic working
	// set; tune further via CACHE_CAPACITY once you know your real shard
	// traffic volume.
	cacheCapacity := 500
	if v := os.Getenv("CACHE_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cacheCapacity = n
		} else {
			fmt.Printf("[%s] WARNING: invalid CACHE_CAPACITY=%q, falling back to %d\n", nodeID, v, cacheCapacity)
		}
	}

	engine := NewTwoQEngine(cacheCapacity)
	srv := NewServer(nodeID, storageDir, engine)

	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("Failed to bind to network %s: %v", bindAddr, err)
	}

	maxSizeOption := grpc.MaxRecvMsgSize(1 << 30)
	maxSendOption := grpc.MaxSendMsgSize(1 << 30)

	grpcServer := grpc.NewServer(maxSizeOption, maxSendOption)

	pb.RegisterCacheServiceServer(grpcServer, srv)
	pb.RegisterReplicationServiceServer(grpcServer, srv)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

		<-sigCh

		fmt.Println("\nShutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server crashed: %v", err)
	}
}

// parseRaftPeers parses a "NodeID=addr,NodeID=addr,..."
// string into a map.
//
// Returns nil when raw is blank. This is used by the two Raft standby
// nodes so they do not bootstrap themselves into the active cluster.
func parseRaftPeers(raw string) map[string]string {
	if raw == "" {
		return nil
	}

	peers := make(map[string]string)

	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)

		if pair == "" {
			continue
		}

		kv := strings.SplitN(pair, "=", 2)

		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			log.Fatalf("invalid -raft-peers entry %q, expected NodeID=addr", pair)
		}

		peers[kv[0]] = kv[1]
	}

	return peers
}

func startRaftNode(nodeID, bindAddr, advertiseAddr string, peers map[string]string, raftSeed string) {
	if advertiseAddr == "" {
		advertiseAddr = bindAddr
	}

	fmt.Printf("[%s] Starting Raft Consensus Node on %s (advertising %s)\n", nodeID, bindAddr, advertiseAddr)

	// Empty peers is the actual signal for "this is a standby" in your setup -
	// main nodes always get RAFT_PEERS, standbys (Raft-06, Raft-07, or
	// Raft-Backup-*, whatever you name them) never do. No naming convention
	// needed.
	isStandby := len(peers) == 0

	if len(peers) == 0 {
		if isStandby {
			fmt.Printf("[%s] Standby Raft node: no active Raft peers configured\n", nodeID)
		} else {
			fmt.Printf("[%s] WARNING: no RAFT_PEERS configured - this node will not join the active Raft cluster\n", nodeID)
		}
	}

	raftInstance, fsm, err := SetupRaftNode(nodeID, bindAddr, advertiseAddr, peers)
	if err != nil {
		log.Fatalf("Failed to initialize Raft node: %v", err)
	}

	// The Raft API (POST /apply, GET /meta, GET /status, POST /join, POST
	// /promote) runs on a separate port from the raw Raft transport. The
	// transport port only speaks Raft's binary protocol.
	apiBindAddr := os.Getenv("RAFT_API_ADDR")

	if apiBindAddr == "" {
		apiBindAddr = deriveAPIAddr(bindAddr)
	}

	apiAdvertiseAddr := os.Getenv("RAFT_API_ADVERTISE")

	if apiAdvertiseAddr == "" {
		apiAdvertiseAddr = deriveAPIAddr(advertiseAddr)
	}

	StartRaftAPI(nodeID, apiBindAddr, raftInstance, fsm, func(leaderRaftAddr string) string {
		return deriveAPIAddr(leaderRaftAddr)
	})

	fmt.Printf("[%s] Raft API bound on %s (advertise %s)\n", nodeID, apiBindAddr, apiAdvertiseAddr)

	// Standby nodes were started with no peers, so they never bootstrapped
	// into any cluster (see SetupRaftNode). They need to ask an existing
	// member to add them as a nonvoter. Without this loop they'd stay
	// isolated forever - this is the piece that actually makes "join when
	// a main node dies" possible; promotion (backend.go's failover watcher
	// or /raft/promote) only works on nodes that already joined as nonvoters.
	if isStandby {
		if raftSeed == "" {
			log.Fatalf("[%s] is a standby node but no -raft-seed/RAFT_SEED was provided (need the raft advertise address of an existing active Raft node)", nodeID)
		}
		seedAPIAddr := deriveAPIAddr(raftSeed)

		go func() {
			for {
				if err := JoinAsNonvoter(seedAPIAddr, nodeID, advertiseAddr); err != nil {
					fmt.Printf("[%s] join attempt via %s failed: %v, retrying in 3s...\n", nodeID, seedAPIAddr, err)
					time.Sleep(3 * time.Second)
					continue
				}
				fmt.Printf("[%s] joined cluster as standby (nonvoter)\n", nodeID)
				return
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh

	fmt.Println("\nShutting down Raft node...")
	raftInstance.Shutdown()
}