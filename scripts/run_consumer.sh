#!/usr/bin/env bash
 /usr/hdp/2.3.0.0-2557/kafka/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic SbtStream --from-beginning