// -----------------------------------------------------------------------
//  <copyright file="SubSourceWithCancellation.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

namespace Akka.Streams.Kafka;

public class PartitionWasRevoked: SubscriptionWithCancelException.NonFailureCancellation
{
    public static readonly PartitionWasRevoked Instance = new ();
    private PartitionWasRevoked() { }
}
