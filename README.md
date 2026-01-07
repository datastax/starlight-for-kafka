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

## CI: Netty leak detection reporting

This repo's CI is able to report Netty `ByteBuf` leak detection messages (and optionally fail the workflow when leaks
are found).

### GitHub Actions (manual run)

The following workflows support `workflow_dispatch` with an input called `netty_leak_detection`:

- `kop tests` (`.github/workflows/pr-tests.yml`)
- `kop mvn build check and kafka-impl test` (`.github/workflows/pr-impl-test.yml`)
- `kop proxy tests` (`.github/workflows/pr-proxy-tests.yml`)
- `docker tests` (`.github/workflows/pr-docker-tests.yml`)

Modes:

- `report` (default): report leaks as warnings in the job logs
- `fail_on_leak`: report leaks and fail the job when leaks are detected
- `off`: disable leak reporting

### Local runs

This project uses a custom Netty leak detector (`ExtendedNettyLeakDetector`) that dumps detected leaks to files named
`netty_leak_*.txt` under `NETTY_LEAK_DUMP_DIR`. The CI step `scripts/report_netty_leaks.sh` reports leaks based on
those dump files.

By default, unit tests run with Netty leak detection level `paranoid` (configurable via `-DtestLeakDetectionLevel`).

To get consistent results locally, set `NETTY_LEAK_DUMP_DIR` before running tests so the detector and the report script
look at the same directory:

```bash
export NETTY_LEAK_DETECTION=report
export NETTY_LEAK_DUMP_DIR=$PWD/target/netty-leak-dumps

mvn test -pl kafka-impl
bash scripts/report_netty_leaks.sh
```

To disable Netty leak detection in the test JVMs, set `NETTY_LEAK_DETECTION=off` before running Maven (this activates a
Maven profile that removes the leak detection JVM args):

```bash
export NETTY_LEAK_DETECTION=off
mvn test -pl kafka-impl
```

For Testcontainers-based tests (module `tests`), setting `NETTY_LEAK_DETECTION` also enables leak detection-related JVM
flags inside the Pulsar/Proxy containers. The leak dumps are written under `NETTY_LEAK_DUMP_DIR/containers/<name>/`.

```bash
export NETTY_LEAK_DETECTION=report
export NETTY_LEAK_DUMP_DIR=$PWD/target/netty-leak-dumps

mvn test -pl tests -DfailIfNoTests=false -Dtest=DockerTest
bash scripts/report_netty_leaks.sh
```

To make Netty leaks fail the check locally, use `fail_on_leak`:

```bash
NETTY_LEAK_DETECTION=fail_on_leak bash scripts/report_netty_leaks.sh
```

For debugging, you can also make the test JVM exit immediately on the first detected leak:

```bash
mvn test -pl kafka-impl -DtestExitJvmOnLeak=true
```
