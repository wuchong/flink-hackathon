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

package org.apache.flink.connectors.kafka.source.enumerator.initializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * A interface for users to specify the starting / stopping offset of a {@link KafkaPartitionSplit}.
 *
 * @see ReaderHandledOffsetsInitializer
 * @see SpecifiedOffsetsInitializer
 * @see TimestampOffsetsInitializer
 */
@PublicEvolving
public interface OffsetsInitializer extends Serializable {

	/**
	 * Get the initial offsets for the given Kafka partitions. These offsets will be used
	 * as either starting offsets or stopping offsets of the Kafka partitions.
	 *
	 * <p>If the implementation returns a starting offset which causes
	 * {@code OffsetsOutOfRangeException} from Kafka. The {@link OffsetResetStrategy}
	 * provided by the {@link #getAutoOffsetResetStrategy()} will be used to reset the
	 * offset.
	 *
	 * @param partitions the Kafka partitions to get the starting offsets.
	 * @param partitionOffsetsRetriever a helper to retrieve information of the Kafka partitions.
	 * @return A mapping from Kafka partition to their offsets to start consuming from.
	 */
	Map<TopicPartition, Long> getPartitionOffsets(
		Collection<TopicPartition> partitions,
		PartitionOffsetsRetriever partitionOffsetsRetriever);

	/**
	 * Get the auto offset reset strategy in case the initialized offsets falls out of the range.
	 *
	 * <p>The OffsetStrategy is only used when the offset initializer is used to initialize the
	 * starting offsets and the starting offsets is out of range.
	 *
	 * @return An {@link OffsetResetStrategy} to use if the initialized offsets are out of the range.
	 */
	OffsetResetStrategy getAutoOffsetResetStrategy();

	/**
	 * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
	 * initial offsets of the Kafka partitions.
	 */
	interface PartitionOffsetsRetriever {

		/**
		 * The group id should be the set for {@link org.apache.flink.connectors.kafka.source.KafkaSource KafkaSource}
		 * before invoking this method. Otherwise an {@code IllegalStateException} will be thrown.
		 *
		 * @see KafkaAdminClient#listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)
		 * @throws IllegalStateException if the group id is not set for the {@code KafkaSource}.
		 */
		Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions);

		/**
		 * @see KafkaConsumer#endOffsets(Collection)
		 */
		Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

		/**
		 * @see KafkaConsumer#beginningOffsets(Collection)
		 */
		Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

		/**
		 * @see KafkaConsumer#offsetsForTimes(Map)
		 */
		Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);
	}

	// --------------- factory methods ---------------

	static OffsetsInitializer committedOffsets() {
		return committedOffsets(OffsetResetStrategy.EARLIEST);
	}

	static OffsetsInitializer committedOffsets(OffsetResetStrategy offsetResetStrategy) {
		return new ReaderHandledOffsetsInitializer(KafkaPartitionSplit.COMMITTED_OFFSET, offsetResetStrategy);
	}

	static OffsetsInitializer timestamps(long timestamp) {
		return new TimestampOffsetsInitializer(timestamp);
	}

	static OffsetsInitializer earliest() {
		return new ReaderHandledOffsetsInitializer(KafkaPartitionSplit.EARLIEST_OFFSET, OffsetResetStrategy.EARLIEST);
	}

	static OffsetsInitializer latest() {
		return new ReaderHandledOffsetsInitializer(KafkaPartitionSplit.LATEST_OFFSET, OffsetResetStrategy.LATEST);
	}

	static OffsetsInitializer offsets(Map<TopicPartition, Long> offsets) {
		return new SpecifiedOffsetsInitializer(offsets, OffsetResetStrategy.EARLIEST);
	}

	static OffsetsInitializer offsets(Map<TopicPartition, Long> offsets, OffsetResetStrategy offsetResetStrategy) {
		return new SpecifiedOffsetsInitializer(offsets, offsetResetStrategy);
	}

	static OffsetsInitializer noStoppingOffsets() {
		return new NoStoppingOffsetsInitializer();
	}
}
