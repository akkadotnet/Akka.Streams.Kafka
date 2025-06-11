// -----------------------------------------------------------------------
//  <copyright file="Program.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace SimpleConsumer;

public class Program
{
    public static void Main(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable("CONNECTIONSTRINGS__KAFKA");
        if(connectionString is null)
            throw new Exception("The environment variable CONNECTIONSTRINGS__KAFKA was not set.");
        
        var fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = DEBUG
                ").WithFallback(
            ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

        var system = ActorSystem.Create("TestKafka", fallbackConfig);
        var materializer = system.Materializer();

        var consumerSettings = ConsumerSettings<string, string>.Create(system, null, null)
            .WithBootstrapServers(connectionString)
            .WithGroupId("group1");

        var subscription = Subscriptions.Topics("akka100");

        KafkaConsumer.PlainSource(consumerSettings, subscription)
            .RunForeach(
                result =>
                {
                    Console.WriteLine(
                        $"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Message.Value}");
                }, materializer);


        Console.ReadLine();
    }
}