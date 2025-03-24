// -----------------------------------------------------------------------
//  <copyright file="DockerSupport.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using Testcontainers.Kafka;

namespace Akka.Streams.Kafka.Cpu.Benchmark;

public class DockerSupport
{
    private KafkaContainer _container = null!;

    public int KafkaPort { get; private set; }

    public string KafkaAddress
    {
        get { return $"{_container.Hostname}:{KafkaPort}"; }
    }

    public int ZookeeperPort { get; private set; }

    public async Task SetupContainersAsync()
    {
        _container = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:6.1.9")
            .WithEnvironment(new Dictionary<string, string>
            {
                ["KAFKA_BROKER_ID"] = "1",
                ["KAFKA_NUM_PARTITIONS"] = "3",
                ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
                ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
                ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
                ["KAFKA_OPTS"] = "-Djava.net.preferIPv4Stack=True"
            })
            .Build();

        await _container.StartAsync();
        ZookeeperPort = _container.GetMappedPublicPort(KafkaBuilder.ZookeeperPort);
        KafkaPort = _container.GetMappedPublicPort(KafkaBuilder.KafkaPort);
    }

    public async Task TearDownDockerAsync() => await _container.DisposeAsync();
}