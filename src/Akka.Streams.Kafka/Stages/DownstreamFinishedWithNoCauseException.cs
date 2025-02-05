using System;

namespace Akka.Streams.Kafka.Stages;

public class DownstreamFinishedWithNoCauseException: Exception
{
    public override string Message => "Downstream flow or stage completed with no cause";
}