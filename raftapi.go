package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

// LogCommand is defined in reedsolomon.go.
// MetadataFSM.Apply in raft.go unmarshals every committed Raft log entry.
func deriveAPIAddr(raftAddr string) string {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		panic(fmt.Sprintf("deriveAPIAddr: invalid address %q: %v", raftAddr, err))
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(fmt.Sprintf("deriveAPIAddr: non-numeric port in %q: %v", raftAddr, err))
	}

	return net.JoinHostPort(host, strconv.Itoa(port+1000))
}

type joinRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// StartRaftAPI serves the HTTP-facing side of a Raft node.
//
// POST /apply
//     Applies a LogCommand through the Raft leader.
//
// GET /meta?id=<fileID>
//     Reads metadata from this node's local FSM.
//
// GET /status
//     Reports node ID, Raft state, leader address and commit index.
//
// POST /join
//     A standby node asks to be added as a nonvoter. Only the leader can do
//     this; a follower redirects like /apply does.
//
// POST /remove
//     Removes a server from the Raft configuration. Leader only.
//
// POST /promote
//     Promotes an existing nonvoter to a full voter. Only the leader can do
//     this; a follower redirects like /apply does.
func StartRaftAPI(nodeID, apiBindAddr string, r *raft.Raft, fsm *MetadataFSM, leaderAPIAddr func(string) string) {
	mux := http.NewServeMux()

	// redirectIfNotLeader writes the "not leader, try here instead" response
	// and returns true if the caller should stop handling the request.
	redirectIfNotLeader := func(w http.ResponseWriter) bool {
		if r.State() == raft.Leader {
			return false
		}

		leaderRaftAddr := string(r.Leader())
		if leaderRaftAddr == "" {
			http.Error(w, "no raft leader elected yet", http.StatusServiceUnavailable)
			return true
		}

		leaderAPI := leaderAPIAddr(leaderRaftAddr)
		w.Header().Set("Location", "http://"+leaderAPI)
		w.Header().Set("X-Raft-Leader", leaderAPI)
		http.Error(w, "not leader", http.StatusMisdirectedRequest)
		return true
	}

	mux.HandleFunc("/apply", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNotLeader(w) {
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		var cmd LogCommand
		if err := json.Unmarshal(body, &cmd); err != nil {
			http.Error(w, "invalid LogCommand JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		future := r.Apply(body, 5*time.Second)

		if err := future.Error(); err != nil {
			http.Error(w, "raft apply failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if fsmErr, ok := future.Response().(error); ok && fsmErr != nil {
			http.Error(w, "fsm apply failed: "+fsmErr.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/meta", func(w http.ResponseWriter, req *http.Request) {
		fileID := req.URL.Query().Get("id")

		if fileID == "" {
			http.Error(w, "missing ?id=", http.StatusBadRequest)
			return
		}

		meta, ok := fsm.Get(fileID)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(meta)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		stats := r.Stats()

		commitIndex := uint64(0)
		if value, ok := stats["commit_index"]; ok {
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				commitIndex = parsed
			}
		}

		term := uint64(0)
		if value, ok := stats["term"]; ok {
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				term = parsed
			}
		}

		state := r.State().String()
		leaderAddr := string(r.Leader())

		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":           nodeID,
			"node_id":      nodeID,
			"state":        state,
			"address":      apiBindAddr,
			"leader_addr":  leaderAddr,
			"is_leader":    r.State() == raft.Leader,
			"commit_index": commitIndex,
			"term":         term,
		})
	})

	// /join: a standby node calls this on any known raft peer, asking to be
	// added as a nonvoter. Nonvoters replicate the log in real time but never
	// vote and never count toward quorum - so adding one can't destabilize
	// the existing cluster.
	mux.HandleFunc("/join", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNotLeader(w) {
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		var jr joinRequest
		if err := json.Unmarshal(body, &jr); err != nil || jr.ID == "" || jr.Address == "" {
			http.Error(w, "invalid join request, need {id, address}", http.StatusBadRequest)
			return
		}

		future := r.AddNonvoter(raft.ServerID(jr.ID), raft.ServerAddress(jr.Address), 0, 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, "add nonvoter failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Printf("[%s] standby %s (%s) joined as nonvoter\n", nodeID, jr.ID, jr.Address)
		w.WriteHeader(http.StatusOK)
	})

	// /promote: upgrades an existing nonvoter to a full voting member. Called
	// by the orchestrator when a main raft node is confirmed dead.
	mux.HandleFunc("/promote", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNotLeader(w) {
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		var jr joinRequest
		if err := json.Unmarshal(body, &jr); err != nil || jr.ID == "" || jr.Address == "" {
			http.Error(w, "invalid promote request, need {id, address}", http.StatusBadRequest)
			return
		}

		future := r.AddVoter(raft.ServerID(jr.ID), raft.ServerAddress(jr.Address), 0, 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, "promote to voter failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Printf("[%s] promoted standby %s (%s) to full voter\n", nodeID, jr.ID, jr.Address)
		w.WriteHeader(http.StatusOK)
	})

	// /remove: removes a (dead) server from the Raft configuration entirely,
	// so it stops counting toward the voter set / quorum. Leader only.
	mux.HandleFunc("/remove", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNotLeader(w) {
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer req.Body.Close()

		var jr joinRequest
		if err := json.Unmarshal(body, &jr); err != nil || jr.ID == "" {
			http.Error(w, "invalid remove request, need {id}", http.StatusBadRequest)
			return
		}

		future := r.RemoveServer(raft.ServerID(jr.ID), 0, 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, "remove server failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Printf("[%s] removed %s from the raft configuration\n", nodeID, jr.ID)
		w.WriteHeader(http.StatusOK)
	})

	go func() {
		if err := http.ListenAndServe(apiBindAddr, mux); err != nil {
			panic(fmt.Sprintf("[%s] Raft API server crashed: %v", nodeID, err))
		}
	}()
}

// SubmitLogCommand sends a LogCommand to a Raft API.
//
// If the selected node is a follower, the Raft API returns 421 and
// supplies the leader API address. We follow that redirect once.
func SubmitLogCommand(apiAddr string, cmd LogCommand) error {
	body, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal LogCommand: %w", err)
	}

	return postWithLeaderRedirect(apiAddr, "/apply", body)
}

// JoinAsNonvoter asks the raft node at seedAPIAddr (any known member - it will
// redirect to the leader itself if needed) to add this node as a nonvoter.
func JoinAsNonvoter(seedAPIAddr, nodeID, advertiseAddr string) error {
	body, err := json.Marshal(joinRequest{ID: nodeID, Address: advertiseAddr})
	if err != nil {
		return fmt.Errorf("marshal join request: %w", err)
	}

	return postWithLeaderRedirect(seedAPIAddr, "/join", body)
}

// PromoteToVoter asks the raft node at apiAddr (any known member) to promote
// nodeID/address from nonvoter to full voter.
func PromoteToVoter(apiAddr, nodeID, address string) error {
	body, err := json.Marshal(joinRequest{ID: nodeID, Address: address})
	if err != nil {
		return fmt.Errorf("marshal promote request: %w", err)
	}

	return postWithLeaderRedirect(apiAddr, "/promote", body)
}

// RemoveFromCluster asks the raft node at apiAddr (any known member) to
// remove nodeID from the Raft configuration.
func RemoveFromCluster(apiAddr, nodeID string) error {
	body, err := json.Marshal(joinRequest{ID: nodeID})
	if err != nil {
		return fmt.Errorf("marshal remove request: %w", err)
	}

	return postWithLeaderRedirect(apiAddr, "/remove", body)
}

// postWithLeaderRedirect POSTs body to http://addr+path, following a single
// "not leader" redirect if the target isn't currently the Raft leader.
func postWithLeaderRedirect(addr, path string, body []byte) error {
	url := "http://" + addr + path
	client := &http.Client{Timeout: 6 * time.Second}

	for attempt := 0; attempt < 2; attempt++ {
		resp, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("post to raft API %s: %w", url, err)
		}

		switch resp.StatusCode {
		case http.StatusOK:
			resp.Body.Close()
			return nil

		case http.StatusMisdirectedRequest:
			location := resp.Header.Get("Location")
			if location == "" {
				if leader := resp.Header.Get("X-Raft-Leader"); leader != "" {
					location = "http://" + leader + path
				}
			}
			resp.Body.Close()

			if location == "" || attempt == 1 {
				return fmt.Errorf("not leader and no usable redirect from %s", url)
			}
			url = location

		default:
			msg, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("raft API %s returned %d: %s", url, resp.StatusCode, string(msg))
		}
	}

	return fmt.Errorf("exceeded retries submitting to raft API")
}