namespace Akka.Streams.Kafka.Testkit.Fixture
{
    public sealed class ContainerInfo
    {
        public ContainerInfo(string imageName, string imageTag, string containerBaseName)
        {
            ImageName = imageName;
            ImageTag = imageTag;
            ContainerBaseName = containerBaseName;
            
        }

        public string ImageName { get; }
        public string ImageTag { get; }
        public string ContainerBaseName { get; }
        public string DockerName => $"{ImageName}:{ImageTag}";
    }
}