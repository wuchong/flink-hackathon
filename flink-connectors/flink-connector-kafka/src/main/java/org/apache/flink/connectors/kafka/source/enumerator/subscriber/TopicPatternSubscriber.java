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
import java.util.regex.Pattern;

import static org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.maybeLog;
import static org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.updatePartitionChanges;

/**
 * A subscriber to a topic pattern.
 */
class TopicPatternSubscriber implements KafkaSubscriber {
	private static final long serialVersionUID = -7471048577725467797L;
	private static final Logger LOG = LoggerFactory.getLogger(TopicPatternSubscriber.class);
	private final Pattern topicPattern;

	TopicPatternSubscriber(Pattern topicPattern) {
		this.topicPattern = topicPattern;
	}

	@Override
	public PartitionChange getPartitionChanges(
		KafkaConsumer<?, ?> consumer,
		Set<TopicPartition> currentAssignment) {
		Set<TopicPartition> newPartitions = new HashSet<>();
		Set<TopicPartition> removedPartitions = new HashSet<>(currentAssignment);

		Map<String, List<PartitionInfo>> topicsInfo = consumer.listTopics();
		for (Map.Entry<String, List<PartitionInfo>> topicEntry : topicsInfo.entrySet()) {
			String topic = topicEntry.getKey();
			if (topicPattern.matcher(topic).matches()) {
				updatePartitionChanges(newPartitions, removedPartitions, topicEntry.getValue());
			}
		}
		maybeLog(newPartitions, removedPartitions, LOG);
		return new PartitionChange(newPartitions, removedPartitions);
	}
}
