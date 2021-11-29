using System;
using System.Diagnostics;
using Akka.Event;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Akka.Streams.Kafka.Testkit
{
    public class MessageSinkLogger : ILoggingAdapter, IDisposable
    {
        private readonly Stopwatch _watch;
        private readonly IMessageSink _sink;

        public MessageSinkLogger(IMessageSink sink)
        {
            _watch = Stopwatch.StartNew();
            _sink = sink;
        }
        
        public void Debug(string message)
            => Log(LogLevel.DebugLevel, message);
        
        public void Debug(string format, params object[] args) 
            => Log(LogLevel.DebugLevel, format, args);

        public void Debug(Exception cause, string format, params object[] args)
            => Log(LogLevel.DebugLevel, cause, format, args);
        
        public void Info(string message)
            => Log(LogLevel.InfoLevel, message);
        
        public void Info(string format, params object[] args)
            => Log(LogLevel.InfoLevel, format, args);

        public void Info(Exception cause, string format, params object[] args) 
            => Log(LogLevel.InfoLevel, cause, format, args);

        public void Warning(string message)
            => Log(LogLevel.WarningLevel, message);
        
        public void Warning(string format, params object[] args)
            => Log(LogLevel.WarningLevel, format, args);

        public void Warning(Exception cause, string format, params object[] args) 
            => Log(LogLevel.WarningLevel, cause, format, args);

        public void Error(string format, params object[] args) 
            => Log(LogLevel.ErrorLevel, format, args);

        public void Error(Exception cause, string format, params object[] args)
            => Log(LogLevel.ErrorLevel, cause, format, args);

        public void Error(string message)
            => Log(LogLevel.ErrorLevel, message);

        public void Dispose()
        {
            _watch.Stop();
        }

        public void Log(LogLevel logLevel, string format, params object[] args)
        {
            _sink?.OnMessage(new DiagnosticMessage($"[XunitFixture][{ToDesc(logLevel)}][{_watch.Elapsed}] {string.Format(format, args)}"));
        }

        public void Log(LogLevel logLevel, Exception cause, string format, params object[] args)
        {
            _sink?.OnMessage(new DiagnosticMessage($"[XunitFixture][{ToDesc(logLevel)}][{_watch.Elapsed}] {string.Format(format, args)}: {cause.Message}:\n{cause.StackTrace}"));
        }

        private string ToDesc(LogLevel level)
            => level switch
            {
                LogLevel.DebugLevel => "DBG",
                LogLevel.InfoLevel => "INF",
                LogLevel.WarningLevel => "WRN",
                LogLevel.ErrorLevel => "ERR",
                _ => throw new IndexOutOfRangeException($"Unknown LogLevel: [{level}]")
            };

        public bool IsEnabled(LogLevel logLevel) => _sink != null;
        public bool IsDebugEnabled => IsEnabled(LogLevel.DebugLevel);
        public bool IsInfoEnabled => IsEnabled(LogLevel.InfoLevel);
        public bool IsWarningEnabled => IsEnabled(LogLevel.WarningLevel);
        public bool IsErrorEnabled => IsEnabled(LogLevel.ErrorLevel);
    }
}