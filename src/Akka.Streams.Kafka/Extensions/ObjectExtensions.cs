// -----------------------------------------------------------------------
//  <copyright file="ObjectExtensions.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Streams.Util;
using Akka.Util;
using Newtonsoft.Json;

namespace Akka.Streams.Kafka.Extensions;

public static class ObjectExtensions
{
    /// <summary>
    /// Returns object's json representation as string
    /// </summary>
    public static string ToJson(this object obj) => JsonConvert.SerializeObject(obj);

    /// <summary>
    /// Wraps object to the option
    /// </summary>
    public static Option<T> AsOption<T>(this T obj) => Option<T>.Create(obj);
}