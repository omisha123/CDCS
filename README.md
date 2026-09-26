# CDCS — Consistent Distributed Cache System

> A fault-tolerant distributed storage and caching system designed to remain available and consistent even when individual nodes fail.

CDCS is a distributed system that combines **Reed-Solomon erasure coding**, **Raft-based metadata replication**, persistent storage, and an orchestration layer to provide reliable file storage across multiple machines.

The system was initially developed and tested on a single machine and was later scaled into a multi-node deployment where individual physical systems can act as storage or coordination nodes.

---

## ✦ What CDCS Does

When a file is uploaded to CDCS, the system:

1. Receives the file through the API gateway.
2. Splits the data into multiple shards.
3. Generates parity shards using **Reed-Solomon erasure coding**.
4. Distributes the shards across storage nodes.
5. Replicates metadata using a **Raft consensus cluster**.
6. Stores the required metadata persistently.
7. Reconstructs missing data when storage nodes become unavailable.

This means the system can continue operating even when individual nodes fail, provided enough shards remain available for reconstruction.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │       Client        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    API Gateway      │
                         │   / Orchestrator    │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
          ┌───────────────────┐           ┌───────────────────┐
          │  Storage Layer    │           │   Metadata Layer  │
          │ Reed-Solomon      │           │      Raft         │
          └─────────┬─────────┘           └─────────┬─────────┘
                    │                               │
          ┌─────────┼─────────┐           ┌─────────┼─────────┐
          ▼         ▼         ▼           ▼         ▼         ▼
       Node-01   Node-02   Node-03     Raft-01   Raft-02   Raft-03
          │         │         │           │         │         │
          ▼         ▼         ▼           └───────┬─┴─────────┘
       Shards    Shards    Shards                  │
                                                   ▼
                                             Replicated
                                              Metadata
```

### Storage Layer

CDCS uses **Reed-Solomon erasure coding** to divide uploaded data into data and parity shards.

For example:

```text
              Original File
                    │
                    ▼
             ┌─────────────┐
             │ Reed-Solomon│
             └──────┬──────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
     Data 1       Data 2      Data 3
        │           │           │
        ▼           ▼           ▼
     Parity 1    Parity 2    Parity 3
```

If a node containing a shard becomes unavailable, the missing data can be reconstructed from the remaining shards.

---

## Metadata Consistency

Storage alone is not enough for a distributed system.

CDCS maintains metadata such as file information, shard locations, and system state using a **Raft consensus cluster**.

```text
                 ┌──────────────┐
                 │   Raft-01    │
                 │    Leader    │
                 └──────┬───────┘
                        │
                replicated log
                 ┌──────┴───────┐
                 ▼              ▼
          ┌────────────┐  ┌────────────┐
          │  Raft-02   │  │  Raft-03   │
          │  Follower  │  │  Follower  │
          └────────────┘  └────────────┘
```

Raft ensures that committed metadata changes are replicated consistently across the active coordination nodes.

---

## Fault Tolerance

CDCS is designed around the assumption that **nodes can fail**.

When a storage node becomes unavailable:

```text
        BEFORE FAILURE

   Node 1   Node 2   Node 3   Node 4
     ✓        ✓        ✓        ✓


        NODE FAILURE

   Node 1   Node 2   Node 3   Node 4
     ✓        ✕        ✓        ✓


        RECONSTRUCTION

   Remaining shards
          │
          ▼
   Reed-Solomon Decoder
          │
          ▼
   Reconstructed shard
```

The system detects unavailable nodes and uses the remaining shards to reconstruct missing data when sufficient information remains.

---

# Key Components

## API Gateway / Orchestrator

The orchestrator acts as the entry point to the system.

It is responsible for coordinating operations between:

* Clients
* Storage nodes
* Reed-Solomon encoding/decoding
* Metadata storage
* Raft nodes
* Monitoring endpoints

---

## Reed-Solomon Storage

Reed-Solomon coding provides data redundancy without requiring a complete copy of every file on every node.

The system separates the file into:

* Data shards
* Parity shards

This allows missing shards to be reconstructed from the surviving shards.

---

## Raft Metadata Cluster

The metadata subsystem uses Raft to maintain a consistent replicated state.

The cluster consists of active Raft nodes and can also support standby/backup coordination nodes in the scaled deployment.

---

## Persistent Metadata

CDCS uses persistent storage for metadata so that system state is not lost when individual processes restart.

---

## Monitoring

CDCS exposes metrics that can be consumed by monitoring systems such as:

* **Prometheus**
* **Grafana**

Metrics can be used to observe:

* Request counts
* Upload/download latency
* Node status
* Reconstruction operations
* Storage operations
* System performance

---

# Deployment

CDCS supports both local development and multi-machine deployment.

## Local Development

The original system was developed to run on a single machine.

This environment is useful for:

* Development
* Debugging
* Testing
* Simulating node failures

---

## Docker Deployment

For the scaled deployment, CDCS is containerized using Docker.

Each physical machine can run a CDCS node without requiring the Go development environment to be installed directly on that machine.

The deployment architecture separates:

```text
                 Orchestrator
                     │
       ┌─────────────┼─────────────┐
       │             │             │
       ▼             ▼             ▼
   Storage        Storage       Storage
    Node           Node          Node
       │             │             │
       └─────────────┼─────────────┘
                     │
                     ▼
                Raft Cluster
```

The node configuration contains the addresses required for the distributed cluster.

---

## Building the Project

Build the CDCS executable:

```bash
go build -o cdcs main.go
```

Build the Docker image:

```bash
docker build -t cdcs:latest .
```

Save the image for offline distribution:

```bash
docker save cdcs:latest -o cdcs.tar
```

The resulting image archive can then be transferred to the machines participating in the deployment.

---

# Running with Docker

Load the Docker image on a target machine:

```bash
docker load -i cdcs.tar
```

Then start the required services using the project's Docker configuration.

The exact deployment configuration depends on the node's assigned role and network address.

---

# API

The system exposes an HTTP API through the orchestrator.

Typical operations include:

```text
POST   /upload
GET    /download
GET    /nodes
GET    /raft/status
GET    /metrics
```

The exact API contract and request/response formats are documented in the project documentation.

---

# Testing Fault Tolerance

One of the main goals of CDCS is to verify that data remains recoverable when nodes fail.

A typical test looks like:

```text
1. Upload a file
        ↓
2. Verify successful storage
        ↓
3. Kill one or more storage nodes
        ↓
4. Request the original file
        ↓
5. Reconstruct missing shards
        ↓
6. Download the reconstructed file
        ↓
7. Compare with original
```

The downloaded file should match the original byte-for-byte when sufficient shards remain available.

---

# Performance & Correctness

CDCS can be evaluated using two broad categories of metrics.

### Correctness

* Upload → download byte-for-byte equality
* Successful reconstruction after node failures
* Metadata consistency across Raft nodes
* Node recovery behavior

### Performance

* Upload latency
* Download latency
* p50 / p95 / p99 request latency
* Throughput
* Reconstruction latency
* Storage-node response time

---

# Project Structure

A simplified view of the project:

```text
CDCS/
│
├── main.go
├── Dockerfile
├── docker-compose.yml
├── cluster.json
│
├── storage/
│   └── ...
│
├── raft/
│   └── ...
│
├── orchestrator/
│   └── ...
│
├── metadata/
│   └── ...
│
└── monitoring/
    └── ...
```

The exact structure may differ between development and scaled deployment versions.

---

# Technology Stack

| Component          | Technology               |
| ------------------ | ------------------------ |
| Language           | Go                       |
| Storage            | Persistent local storage |
| Erasure Coding     | Reed-Solomon             |
| Consensus          | HashiCorp Raft           |
| Metadata           | Pebble                   |
| Containerization   | Docker                   |
| Metrics            | Prometheus               |
| Visualization      | Grafana                  |
| API                | HTTP                     |
| Node Communication | gRPC / HTTP              |

---

# Documentation

For detailed architecture, deployment instructions, APIs, fault-tolerance experiments, and implementation details:

**📚 [CDCS Documentation Wiki](https://github.com/omisha123/CDCS/wiki/Documentation)**

---

# Why CDCS?

CDCS explores how several distributed-systems concepts work together in a single system:

```text
        ┌─────────────────────────────┐
        │       Distributed System    │
        └──────────────┬──────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   Reed-Solomon      Raft         Orchestration
   Fault Tolerance   Consensus     Coordination
        │              │              │
        └──────────────┼──────────────┘
                       ▼
              Reliable Storage
```

The project combines **data redundancy**, **metadata consensus**, **persistent storage**, **node failure handling**, and **multi-machine deployment** into one distributed system.

---

## Status

🚧 **Active Development**

CDCS is being continuously improved as the system moves from local development toward a fully distributed multi-machine deployment.
