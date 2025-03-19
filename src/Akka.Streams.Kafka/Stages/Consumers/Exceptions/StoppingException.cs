// -----------------------------------------------------------------------
//  <copyright file="StoppingException.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;

namespace Akka.Streams.Kafka.Stages.Consumers.Exceptions;

/// <summary>
/// Thrown in response to commit/message request commands by consuming actor when in stopping state
/// </summary>
public sealed class StoppingException() : Exception("Kafka consumer is stopping");