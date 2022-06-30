# Starlight for Kafka

This repository contains the code for Starlight for Kafka.

Starlight for Kafka allows your Apache Kafka® clients to connect to an Apache Pulsar® cluster.

Starlight for Kafka brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. By adding the Starlight for Kafka protocol handler to your existing Pulsar cluster, you can migrate your existing Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s powerful features, such as:

- Streamlined operations with enterprise-grade multi-tenancy
- Simplified operations with a rebalance-free architecture
- Infinite event stream retention with Apache BookKeeper and tiered storage
- Serverless event processing with Pulsar Functions

Starlight for Kafka, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with the protocol name "kafka", is loaded when Pulsar broker starts. This reduces the barriers for people adopting Pulsar to achieve business success by providing a native Kafka protocol support on Apache Pulsar. By integrating two popular event streaming ecosystems, Starlight for Kafka unlocks new use cases. Leverage advantages from each ecosystem and build a truly unified event streaming platform with Apache Pulsar to accelerate the development of real-time applications and services.

Starlight for Kafka implements the Kafka wire protocol on Pulsar by leveraging the existing components (such as topic discovery, the distributed log library - ManagedLedger, cursors and so on) that Pulsar already has.

## Features

Starlight for Kafka adds additional features to make native Kafka protocol support even easier. 

* A schema registry compatible with both the [Confluent Schema Registry®](https://docs.confluent.io/platform/current/schema-registry/index.html) and the [Apicurio Schema Registry](https://www.apicur.io/registry). 

* A proxy extension allowing the Kafka client to access your Pulsar cluster the same way as Pulsar clients do. 

* Integrated support for the Pulsar schema registry.

For documentation, see the [Starlight for Kafka documentation](https://docs.datastax.com/en/starlight-kafka/docs/1.0/index.html).



