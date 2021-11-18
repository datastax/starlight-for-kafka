FROM ubuntu:latest
RUN apt-get update && apt-get install -y curl
RUN mkdir /confluent \
      && cd /confluent \
      && curl -O http://packages.confluent.io/archive/7.0/confluent-7.0.0.tar.gz \
      && tar zxvf confluent-7.0.0.tar.gz --strip-components 1
RUN apt-get install -y openjdk-11-jdk
