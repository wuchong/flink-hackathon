/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connectors.kafka.source.enumerator.subscriber;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.maybeLog;
import static org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.updatePartitionChanges;

/**
 * A subscriber to a fixed list of topics.
 */
class TopicListSubscriber implements KafkaSubscriber {
	private static final long serialVersionUID = -6917603843104947866L;
	private static final Logger LOG = LoggerFactory.getLogger(TopicListSubscriber.class);
	private final List<String> topics;

	TopicListSubscriber(List<String> topics) {
		this.topics = topics;
	}

	@Override
	public PartitionChange getPartitionChanges(
		KafkaConsumer<?, ?> consumer,
		Set<TopicPartition> currentAssignment) {
		Set<TopicPartition> newPartitions = new HashSet<>();
		Set<TopicPartition> removedPartitions = new HashSet<>(currentAssignment);

		// We cannot call partitionsFor() directly on each topic because this may accidentally create the topic
		// if the topic does not exist. In Kafka 2.3+ we can use the "allow.auto.create.topics" configuration on
		// the consumer side to avoid this.
		Map<String, List<PartitionInfo>> topicMetadata = consumer.listTopics();
		topics.forEach(t -> {
			List<PartitionInfo> partitions = topicMetadata.get(t);
			if (partitions != null) {
				updatePartitionChanges(newPartitions, removedPartitions, partitions);
			}
		});
		maybeLog(newPartitions, removedPartitions, LOG);
		return new PartitionChange(newPartitions, removedPartitions);
	}
}
