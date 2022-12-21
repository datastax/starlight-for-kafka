FROM us-central1-docker.pkg.dev/datastax-gcp-pulsar/pulsar/lunastreaming-all:latest-210
USER root
RUN apt-get update && apt-get -y install openjdk-11-dbg
RUN rm /pulsar/protocols/*.nar /pulsar/proxyextensions/*.nar
ADD async-profiler-2.9-linux-x64.tar.gz /pulsar/
ADD *proxy*nar /pulsar/proxyextensions/
ADD *handler*nar /pulsar/protocols/
