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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A initializer that initialize the partitions to the earliest / latest / last-committed offsets.
 * The offsets initialization are taken care of by the {@code KafkaPartitionSplitReader} instead
 * of by the {@code KafkaSourceEnumerator}.
 *
 * <P>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class ReaderHandledOffsetsInitializer implements OffsetsInitializer {
	private static final long serialVersionUID = 172938052008787981L;
	private final long startingOffset;
	private final OffsetResetStrategy offsetResetStrategy;

	/**
	 * The only valid value for startingOffset is following.
	 * {@link org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit#EARLIEST_OFFSET EARLIEST_OFFSET},
	 * {@link org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit#LATEST_OFFSET LATEST_OFFSET},
	 * {@link org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit#COMMITTED_OFFSET COMMITTED_OFFSET}
	 */
	ReaderHandledOffsetsInitializer(long startingOffset, OffsetResetStrategy offsetResetStrategy) {
		this.startingOffset = startingOffset;
		this.offsetResetStrategy = offsetResetStrategy;
	}

	@Override
	public Map<TopicPartition, Long> getPartitionOffsets(
		Collection<TopicPartition> partitions,
		PartitionOffsetsRetriever partitionOffsetsRetriever) {
		Map<TopicPartition, Long> initialOffsets = new HashMap<>();
		for (TopicPartition tp : partitions) {
			initialOffsets.put(tp, startingOffset);
		}
		return initialOffsets;
	}

	@Override
	public OffsetResetStrategy getAutoOffsetResetStrategy() {
		return offsetResetStrategy;
	}
}
