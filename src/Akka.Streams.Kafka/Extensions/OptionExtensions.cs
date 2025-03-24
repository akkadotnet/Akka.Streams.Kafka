// -----------------------------------------------------------------------
//  <copyright file="OptionExtensions.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Streams.Util;
using Akka.Util;

namespace Akka.Streams.Kafka.Extensions;

public static class OptionExtensions
{
    /// <summary>
    /// Gets option value, if any - otherwise returns default value provided
    /// </summary>
    public static T GetOrElse<T>(this Option<T> option, T defaultValue) =>
        option.HasValue ? option.Value : defaultValue;
}