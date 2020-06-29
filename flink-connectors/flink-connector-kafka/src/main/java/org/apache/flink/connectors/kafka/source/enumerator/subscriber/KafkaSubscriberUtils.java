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

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * The base implementations of {@link KafkaSubscriber}.
 */
class KafkaSubscriberUtils {

	private KafkaSubscriberUtils() {

	}

	static void updatePartitionChanges(
		Set<TopicPartition> newPartitions,
		Set<TopicPartition> removedPartitions,
		List<PartitionInfo> partitionInfoList) {
		for (PartitionInfo pi : partitionInfoList) {
			TopicPartition tp = new TopicPartition(pi.topic(), pi.partition());
			if (!removedPartitions.remove(tp)) {
				newPartitions.add(tp);
			}
		}
	}

	static void maybeLog(Set<TopicPartition> newPartitions, Set<TopicPartition> removedPartitions, Logger logger) {
		if (!removedPartitions.isEmpty()) {
			logger.warn("The following partitions have been removed from the Kafka cluster. {}", removedPartitions);
		}
		if (!newPartitions.isEmpty()) {
			logger.info("The following partitions have been added to the Kafka cluster. {}", newPartitions);
		}
	}
}
