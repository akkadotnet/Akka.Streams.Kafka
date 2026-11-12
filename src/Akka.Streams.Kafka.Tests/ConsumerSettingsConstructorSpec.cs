// -----------------------------------------------------------------------
//  <copyright file="ConsumerSettingsConstructorSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Kafka.Tests;

public class ConsumerSettingsConstructorSpec
{
    [Fact]
    [Obsolete("This test verifies the obsolete constructor remains functional.")]
    public void Legacy_constructor_must_initialize_settings_like_current_constructor()
    {
        var keyDeserializer = Deserializers.Utf8;
        var valueDeserializer = Deserializers.Int32;
        var properties = ImmutableDictionary<string, string>.Empty.Add("group.id", "test-group");
        var checker = ConnectionCheckerSettings.Create(7, TimeSpan.FromSeconds(17), 1.5);

        var legacy = new ConsumerSettings<string, int>(
            keyDeserializer, valueDeserializer, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(6),
            TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(9), TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(11), true, 42, "test-dispatcher", properties, checker);
        var current = new ConsumerSettings<string, int>(
            keyDeserializer, valueDeserializer, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(6),
            TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(9), TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(11), true, 42, "test-dispatcher", properties, checker, null);

        legacy.Properties.Should().BeSameAs(properties);
        legacy.Should().Be(current);
        legacy.GetProperty("group.id").Should().Be("test-group");
        legacy.ConsumerFactory.Should().BeNull();
    }
}
