using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Util;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Dsl;

public class ControlSpec
{
    private class ControlImpl : IControl
    {
        public AtomicBoolean ShutdownCalled { get; }= new(false);
        
        public Task Stop() => Task.CompletedTask;

        public Task Shutdown()
        {
            ShutdownCalled.Value = true;
            return Task.CompletedTask;
        }

        public Task IsShutdown => throw new NotImplementedException();

        public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) =>
            this.DrainAndShutdownDefaultAsync(streamCompletion);
    }

    [Fact]
    public async Task Control_should_drain_to_stream_result()
    {
        var control = new ControlImpl();
        
        var drainingControl = DrainingControl.Create(control, Task.FromResult("expected"));
        var result = await drainingControl.DrainAndShutdown();
        Assert.Equal("expected", result);
        Assert.True(control.ShutdownCalled.Value);
    }
    
    [Fact]
    public async Task Control_should_drain_to_stream_failure()
    {
        var control = new ControlImpl();
        
        var drainingControl = DrainingControl.Create(control, Task.FromException<string>(new ApplicationException("expected")));
        await Assert.ThrowsAsync<ApplicationException>(() => drainingControl.DrainAndShutdown());
        Assert.True(control.ShutdownCalled.Value);
    }
}