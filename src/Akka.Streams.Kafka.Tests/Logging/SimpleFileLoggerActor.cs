using System;
using System.IO;
using Akka.Actor;
using Akka.Event;

namespace Akka.Streams.Kafka.Tests.Logging
{
    public class SimpleFileLoggerActor : ReceiveActor
    {
        public readonly string LogPath = $"logs\\{DateTime.Now:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid():N}.txt";
        
        public SimpleFileLoggerActor()
        {
            Receive<Debug>(e => this.Log(LogLevel.DebugLevel, e.ToString()));
            Receive<Info>(e => this.Log(LogLevel.InfoLevel, e.ToString()));
            Receive<Warning>(e => this.Log(LogLevel.WarningLevel, e.ToString()));
            Receive<Error>(e => this.Log(LogLevel.ErrorLevel, e.ToString()));
            Receive<InitializeLogger>(_ =>
            {
                Sender.Tell(new LoggerInitialized());
            });
        }

        private void Log(LogLevel level, string str)
        {
            var fullPath = Path.GetFullPath(LogPath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath));
            File.AppendAllText(fullPath, $"{str}\r\n");
        }
    }
}