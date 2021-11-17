using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;
using FakeItEasy;

namespace Akka.Streams.Kafka.Tests.Internal
{
    public static class MockConsumer
    {
        public static readonly TimeSpan CloseTimeout = TimeSpan.FromMilliseconds(500);
    }
    
    public class MockConsumer<TKey, TValue>
    {
        private readonly object _lock = new object();
        private readonly List<ConsumeResult<TKey, TValue>> _responses = new List<ConsumeResult<TKey, TValue>>();
        private ImmutableList<string> _pendingSubscriptions = ImmutableList<string>.Empty;
        private ImmutableHashSet<TopicPartition> _assignment = ImmutableHashSet<TopicPartition>.Empty;
        private readonly Dictionary<TopicPartition, bool> _paused = new Dictionary<TopicPartition, bool>();
        private AtomicBoolean _releaseCommitCallbacks = new AtomicBoolean();
        
        public IConsumer<TKey, TValue> Mock { get; protected set; }
        internal ConsumerSettings<TKey, TValue> Settings { get; set; }

        public MockConsumer()
        {
            Mock = A.Fake<IConsumer<TKey, TValue>>();

            A.CallTo(() => Mock.Consume(A<int>.Ignored))
                .ReturnsLazily(() =>
                {
                    lock (_lock)
                    {
                        if (_pendingSubscriptions.Count > 0)
                        {
                            var tps = _pendingSubscriptions.Select(topic => new TopicPartition(topic, 1)).ToList();
                            foreach (var tp in tps)
                            {
                                if(!_paused.ContainsKey(tp))
                                    _paused[tp] = false;
                                _assignment = _assignment.Add(tp);
                            }

                            Settings.RebalanceListener?.OnPartitionAssigned(Mock, tps);
                            _pendingSubscriptions = ImmutableList<string>.Empty;
                            return null;
                        }

                        ConsumeResult<TKey, TValue> result = null;
                        foreach (var response in _responses)
                        {
                            var contained = _assignment.Contains(response.TopicPartition);
                            var exists = _paused.TryGetValue(response.TopicPartition, out var paused);
                            if ( contained && exists && !paused)
                            {
                                result = response;
                                break;
                            }
                        }

                        if (result != null)
                            _responses.Remove(result);
                        
                        return result;
                    }
                });

            A.CallTo(() => Mock.Commit(A<IEnumerable<TopicPartitionOffset>>._))
                .Invokes(tpos =>
                {
                    // TODO: call commit logging callbacks
                });

            A.CallTo(() => Mock.Subscribe(A<IEnumerable<string>>._))
                .Invokes(info =>
                {
                    var topics = (IEnumerable<string>) info.Arguments[0];
                    lock (_lock)
                    {
                        _pendingSubscriptions = topics.ToImmutableList();
                    }
                });

            A.CallTo(() => Mock.Resume(A<IEnumerable<TopicPartition>>._))
                .Invokes(info =>
                {
                    var tps = (IEnumerable<TopicPartition>) info.Arguments[0];
                    lock (_lock)
                    {
                        foreach (var tp in tps)
                        {
                            if(_paused.ContainsKey(tp))
                                _paused[tp] = false;
                        }
                    }
                });
            
            A.CallTo(() => Mock.Pause(A<IEnumerable<TopicPartition>>._))
                .Invokes(info =>
                {
                    var tps = (IEnumerable<TopicPartition>) info.Arguments[0];
                    lock (_lock)
                    {
                        foreach (var tp in tps)
                        {
                            if(_paused.ContainsKey(tp))
                                _paused[tp] = true;
                        }
                    }
                });
            
            A.CallTo(() => Mock.Assignment)
                .ReturnsLazily(() =>
                {
                    lock (_lock)
                    {
                        return _assignment.ToList();
                    }
                });

            A.CallTo(() => Mock.Handle)
                .Returns(null);
        }

        public void Enqueue(List<ConsumeResult<TKey, TValue>> records)
        {
            lock (_lock)
            {
                _responses.AddRange(records);
            }
        }

        public void VerifyClosed()
        {
            A.CallTo(() => Mock.Close())
                .MustHaveHappenedOnceExactly();
        }

        public void VerifyNotClosed()
        {
            A.CallTo(() => Mock.Close())
                .MustNotHaveHappened();
        }
    }

    public class FailingMockConsumer<TKey, TValue> : MockConsumer<TKey, TValue>
    {
        public FailingMockConsumer(Exception exception, int failOnCallNumber)
        {
            Mock = A.Fake<IConsumer<TKey, TValue>>();

            A.CallTo(() => Mock.Handle)
                .Returns(null);
            
            if (failOnCallNumber == 1)
                A.CallTo(() => Mock.Consume(A<int>.Ignored))
                    .Throws(exception);
            else
                A.CallTo(() => Mock.Consume(A<int>.Ignored))
                    .Returns(new ConsumeResult<TKey, TValue>()).NumberOfTimes(failOnCallNumber - 1)
                    .Then
                    .Throws(exception);
        }
    }
}