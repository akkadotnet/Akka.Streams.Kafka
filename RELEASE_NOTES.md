#### 1.5.39-beta2 March 14th 2025 ####

* [Upgraded to Akka.NET v1.5.39](https://github.com/akkadotnet/akka.net/releases/tag/1.5.39)
* [Resolved: Kafka Producer - Exception occured inside SelectAsync - Cancellation cause must not be null](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/426)

#### 1.5.39-beta1 March 13th 2025 ####

*v1.5.39 is a major update for Akka.Streams.Kafka*

* [Resolved: System.ArgumentException: Unexpected records polled potentially thrown during a rebalance](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415)
* [Expose `ConsumerSettings.MaxPollRecords`](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/453) available so users can performance-tune how many records to fetch during polling.
* [Change `Assign` and `AssignWithOffsets` to use `IncrementalAssign`](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/455) - prevents `Offset` resets for users running `ManualSubscription`s
* [Refactor `SubSourceStageLogic`; filter messages from revoked partitions in partitioned stream sources](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/452)
* [Filter out buffered records from recently revoked partitions](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/450)
* [Enable nullability](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/449)