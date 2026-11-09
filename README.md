# Load Balancer – Algorithm Comparison and Performance Analysis

## Overview

This project explores the design, implementation, and performance evaluation of load balancing algorithms in a simulated network environment.

Using **Mininet and Ryu**, we implemented and compared three load balancing strategies:

- **IP Hash** – Static load balancing based on client IP addresses.
- **Least Connections** – Dynamic load balancing based on the number of active connections.
- **Least Connections – Second Tie** – A modified version of Least Connections that selects the second server when connection counts are equal.

The main objective was to investigate how server performance, traffic patterns, client distribution, and tie-breaking policies affect load balancing efficiency.

## Technologies & Tools

- **Mininet** – Network emulation
- **Ryu** – SDN controller framework
- **Linux Networking**
- **Load Balancing Algorithms**
- **Network Performance Analysis**

## Network Topology

The simulated environment consists of:

- Three clients generating network requests.
- A load balancer responsible for distributing incoming traffic.
- Two backend servers (h3 and h4) processing client requests.

**Network Flow:**

Clients → Load Balancer → Backend Servers (h3, h4)

## Implemented Algorithms

### 1. IP Hash

A static load balancing algorithm that assigns clients to backend servers based on their IP addresses.

### 2. Least Connections

A dynamic algorithm that forwards each incoming request to the server with the fewest active connections.

### 3. Least Connections – Second Tie

An alternative implementation of Least Connections developed to investigate the impact of tie-breaking decisions.

When both servers have the same number of active connections, this version selects the second server instead of the first.

## Experiments

We evaluated the algorithms under different network conditions:

- Symmetric servers with identical processing speeds.
- Heterogeneous servers with different processing speeds.
- Different client-to-server IP mappings.
- Different intervals between incoming requests.
- Alternative tie-breaking strategies.

The main performance metric was **Total Completion Time (TCT)**, measuring the time required to complete all client requests.

We also examined average TCT and standard deviation across repeated experiments.

## Key Results & Findings

The experiments demonstrated that load balancing performance depends on server capabilities, traffic patterns, client distribution, and implementation decisions.

Key observations include:

- **Least Connections** generally outperformed IP Hash when backend servers had different processing speeds.
- **IP Hash** could outperform Least Connections when client-to-server mapping favored the faster server.
- **Request timing and packet failures** influenced the experimental results.
- **Tie-breaking policies** significantly affected performance, especially in heterogeneous server environments.
- By modifying the tie-breaking behavior of Least Connections, we achieved approximately **49% lower Total Completion Time (TCT)** compared to the original implementation in one of our experimental scenarios.

These findings highlight how small implementation decisions can significantly impact network performance.

## Conclusion

This project demonstrates that effective load balancing depends not only on the chosen algorithm but also on network conditions, server characteristics, traffic distribution, and implementation details.

By introducing and evaluating an alternative tie-breaking policy, we demonstrated how a small change in routing decisions can significantly improve performance under specific conditions.

## Documentation

Detailed experiments, results, and analysis are available in the final project report.

## Team

- Alaa Abo Mokh
- Adan Abo Mokh
- Mayyas Iktilat

**Course:** Computer Networks Project – Technion, Winter 2026
