using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
    .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

var producer = builder.AddProject<SimpleProducer>("akka-producer")
    .WaitFor(kafka)
    .WithReference(kafka);

builder.AddProject<SimpleConsumer>("akka-consumer")
    .WaitFor(producer)
    .WithReference(kafka);

builder.Build().Run();