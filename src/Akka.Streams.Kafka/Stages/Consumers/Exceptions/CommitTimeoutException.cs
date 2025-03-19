// -----------------------------------------------------------------------
//  <copyright file="CommitTimeoutException.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions;

/// <summary>
/// Commit attempts will be failed with this exception if
/// Kafka doesn't respond within commit timeout
/// </summary>
public sealed class CommitTimeoutException : TimeoutException
{
    public CommitTimeoutException(string message) : base(message)
    {
    }
}