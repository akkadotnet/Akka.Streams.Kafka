// -----------------------------------------------------------------------
//  <copyright file="DownstreamFinishedWithNoCauseException.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;

namespace Akka.Streams.Kafka.Stages;

public class DownstreamFinishedWithNoCauseException : Exception
{
    public override string Message
    {
        get { return "Downstream flow or stage completed with no cause"; }
    }
}