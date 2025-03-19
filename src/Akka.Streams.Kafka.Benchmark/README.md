# Akka.Streams.Kafka Benchmarks

This directory contains benchmarks for measuring the performance of Akka.Streams.Kafka.

## Prerequisites

- Docker and Docker Compose installed
- .NET 8.0 SDK or later
- PowerShell 7.0 or later

## Running the Benchmarks

1. Start Kafka infrastructure:
   ```powershell
   docker-compose up -d
   ```

2. Wait for Kafka to be ready (usually takes 30-60 seconds)

3. Run benchmarks:
   ```powershell
   dotnet run -c Release
   ```

4. When done, stop Kafka:
   ```powershell
   docker-compose down
   ```

## Benchmark Categories

### Consumer Benchmarks

- `PlainConsumerBenchmark`: Measures throughput of plain Kafka consumer
- `CommittableConsumerBenchmark`: Measures throughput with commit tracking
- `BatchCommitBenchmark`: Measures throughput with batch commits

### Producer Benchmarks

- `PlainProducerBenchmark`: Measures throughput of plain Kafka producer

## Configuration

The benchmarks use the following Kafka configuration:

- Bootstrap Servers: localhost:29092
- Topic Partitions: 3
- Replication Factor: 1

Each benchmark uses unique topics and consumer groups to avoid interference. 