using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using Docker.DotNet;
using Docker.DotNet.Models;
using Config = Akka.Configuration.Config;
using DockerConfig = Docker.DotNet.Models.Config;

namespace Akka.Streams.Kafka.Benchmark
{
    public abstract class BenchmarkBase
    {
        public readonly DockerSupport Docker;
        public ActorSystem ActorSystem { get; private set; }
        
        protected BenchmarkBase()
        {
            Docker = new DockerSupport();
        }
        
        public async Task SetupKafkaAsync()
        {
            _uuid = Guid.NewGuid().ToString();
            KafkaTopic = $"topic-1-{_uuid}";
            KafkaGroup = $"group-1-{_uuid}";
            
            await Docker.SetupContainersAsync();
            await Docker.WaitForKafkaServerAsync();
        }

        public async Task SetupAkkaAsync()
        {
            await SetupActorSystemsAsync();
        }

        public async Task TearDownAkkaAsync()
        {
            await TeardownActorSystemsAsync();
        }
        
        public async Task TearDownKafkaAsync()
        {
            await Docker.TearDownDockerAsync();
        }

        #region Akka methods

        private string _uuid;
        //public ActorSystem ProducerSystem { get; private set; }
        public ActorSystem ConsumerSystem { get; private set; }
        public string KafkaTopic { get; private set; }
        public string KafkaGroup { get; private set; }

        private async Task SetupActorSystemsAsync()
        {
            Console.WriteLine("Starting Akka ActorSystems");
            
            //var config = ConfigurationFactory.ParseString("akka.loglevel = DEBUG");
            var config = ConfigurationFactory.ParseString(@"
          akka {
            log-config-on-start = off
            stdout-loglevel = INFO
            loglevel = ERROR
            actor {
              debug {
                  receive = on
                  autoreceive = on
                  lifecycle = on
                  event-stream = on
                  unhandled = on
              }
            }          
          }")
                .WithFallback(KafkaExtensions.DefaultSettings);
            
            //ProducerSystem = ActorSystem.Create("akka-kafka-producer", config);
            ConsumerSystem = ActorSystem.Create("akka-kafka-consumer", config);
            Console.WriteLine("ActorSystems started");
            
            /*
            var producerSettings = ProducerSettings<Null, string>
                .Create(ProducerSystem, null, null)
                .WithBootstrapServers(Docker.KafkaAddress);
            
            Console.WriteLine("Creating Kafka messages");
            // Prime kafka with messages
            Source
                .From(Enumerable.Range(1, 100000))
                .Select(elem => new ProducerRecord<Null, string>(KafkaTopic, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), ProducerSystem.Materializer());
                
            Console.WriteLine("ActorSystems created");
            */
        }

        private async Task TeardownActorSystemsAsync()
        {
            /*
            await Task.WhenAll(
                ProducerSystem.Terminate(),
                ConsumerSystem.Terminate());
                */
            try
            {
                await ConsumerSystem.Terminate();
            } catch {}
        }

        #endregion

    }
}