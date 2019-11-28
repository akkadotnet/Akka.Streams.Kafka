#### 1.0.0 November 27 2019 ####
**First beta release of Akka.Streams.Kafka**

In this release, library was rewritten almost completely.
Conceptually, the main part of this release is implementing all consuming and producing stages supported in Alpakka project: https://github.com/akka/alpakka.
See https://github.com/akkadotnet/Akka.Streams.Kafka/issues/36 for full list of implemented stages (there are 11 consumer and 3 producer stages implemented).

Transactional stages are partially implemented, but waiting for issue https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85 to be resolved 
(which in turn is waiting for issue in Confluent driver to be closed).

Among the others improvements, some critical issues were resolved, like
- Get rid of internal buffering and scheduled pooling in Consumer stages (https://github.com/akkadotnet/Akka.Streams.Kafka/issues/35)
- DrainControl class is implement, which allows to shutdown stages from outside (so does not require source to be finished to stop processing)