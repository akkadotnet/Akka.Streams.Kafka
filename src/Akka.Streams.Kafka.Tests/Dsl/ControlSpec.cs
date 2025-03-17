using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Helpers;
using Akka.Util;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Dsl;

public class ControlSpec
{
    private class ControlImpl : IControl
    {
        private readonly Task _stopTask;
        private readonly Task _shutdownTask;

        public ControlImpl(Task? stopTask = null, Task? shutdownTask = null)
        {
            _stopTask = stopTask ?? Task.CompletedTask;
            _shutdownTask = shutdownTask ?? Task.CompletedTask;
        }

        public AtomicBoolean ShutdownCalled { get; }= new(false);

        public Task Stop() => _stopTask;

        public Task Shutdown()
        {
            ShutdownCalled.Value = true;
            return _shutdownTask;
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
        var e = await Assert.ThrowsAsync<ApplicationException>(() => drainingControl.DrainAndShutdown());
        Assert.Equal("expected", e.Message);
        Assert.True(control.ShutdownCalled.Value);
    }
    
    [Fact]
    public async Task Control_should_drain_to_stream_failure_even_if_shutdown_fails()
    {
        var control = new ControlImpl(shutdownTask:Task.FromException(new ApplicationException("not this")));
        
        var drainingControl = DrainingControl.Create(control, Task.FromException<string>(new ApplicationException("expected")));
        var e = await Assert.ThrowsAsync<ApplicationException>(() => drainingControl.DrainAndShutdown());
        Assert.Equal("expected", e.Message);
        Assert.True(control.ShutdownCalled.Value);
    }

    [Fact]
    public async Task Control_should_drain_to_shutdown_failure_when_stream_succeeds()
    {
        var control = new ControlImpl(shutdownTask:Task.FromException(new ApplicationException("expected")));
        
        var drainingControl = DrainingControl.Create(control, Task.FromResult("expected"));
        var e = await Assert.ThrowsAsync<ApplicationException>(() => drainingControl.DrainAndShutdown());
        Assert.Equal("expected", e.Message);
        Assert.True(control.ShutdownCalled.Value);
    }
}