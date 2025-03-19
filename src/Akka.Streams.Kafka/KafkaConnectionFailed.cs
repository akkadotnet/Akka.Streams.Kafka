// -----------------------------------------------------------------------
//  <copyright file="KafkaConnectionFailed.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Streams.Kafka;

public sealed class KafkaConnectionFailed : Exception
{
    public KafkaConnectionFailed(Exception exception, int attempts) : base(
        $"Can't establish connection with kafkaBroker after {attempts} attempts",
        exception)
    {
    }
}