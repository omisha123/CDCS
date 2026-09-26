package main

import (
	"bytes"
	"fmt"

	"github.com/klauspost/reedsolomon"
)
// LogCommand defines operations for the Raft network
type LogCommand struct {
	Operation string `json:"operation"`
	Payload   []byte `json:"payload"`
}
const (
	rsDataShards   = 4
	rsParityShards = 2
)

// encodeFileToShards mathematically slices the raw byte data into 4 distinct data shards 
// and calculates 2 additional parity shards for fault tolerance[cite: 5].
func encodeFileToShards(data []byte) ([][]byte, error) {
	enc, err := reedsolomon.New(rsDataShards, rsParityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Reed-Solomon encoder: %w", err)
	}

	// Calculate uniform shard size and pad the raw byte array so it divides evenly[cite: 5]
	shardSize := (len(data) + rsDataShards - 1) / rsDataShards
	paddedSize := shardSize * rsDataShards
	
	paddedData := make([]byte, paddedSize)
	copy(paddedData, data)

	shards, err := enc.Split(paddedData)
	if err != nil {
		return nil, fmt.Errorf("failed to split data into shards: %w", err)
	}

	// Compute the 2 parity shards on top of the padded data[cite: 5]
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("failed to encode parity shards: %w", err)
	}

	return shards, nil
}

// reconstructAndJoin evaluates the 6 shards (where missing shards are nil)[cite: 5].
// As long as up to 2 shards are missing, it rebuilds them, stitches the data together, 
// and trims the padding back to the original size[cite: 5].
func reconstructAndJoin(shards [][]byte, originalSize int) ([]byte, error) {
	enc, err := reedsolomon.New(rsDataShards, rsParityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Reed-Solomon encoder: %w", err)
	}

	// Mathematically rebuild the missing pieces if possible[cite: 5]
	if err := enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstruction failed (too many lost shards): %w", err)
	}

	// Verify the integrity of the rebuilt shards
	ok, err := enc.Verify(shards)
	if !ok || err != nil {
		return nil, fmt.Errorf("shard verification failed after reconstruction")
	}

	// Stitch the data shards together and trim the padding back to the originalSize[cite: 5]
	out := new(bytes.Buffer)
	if err := enc.Join(out, shards, originalSize); err != nil {
		return nil, fmt.Errorf("failed to join data shards: %w", err)
	}

	return out.Bytes(), nil
}