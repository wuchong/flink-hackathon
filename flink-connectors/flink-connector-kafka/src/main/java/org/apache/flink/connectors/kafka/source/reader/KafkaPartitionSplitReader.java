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

package org.apache.flink.connectors.kafka.source.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connectors.kafka.source.KafkaSourceOptions;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A {@link SplitReader} implementation that reads records from Kafka partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 *
 * @param <T> the type of the record to be emitted from the Source.
 */
public class KafkaPartitionSplitReader<T> implements SplitReader<Tuple3<T, Long, Long>, KafkaPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionSplitReader.class);
	private static final Future<Void> READY = CompletableFuture.completedFuture(null);

	private final KafkaConsumer<byte[], byte[]> consumer;
	private final KafkaDeserializer<T> deserializationSchema;
	private final Map<TopicPartition, Long> stoppingOffsets;
	private final SimpleCollector<T> collector;
	private final String groupId;

	public KafkaPartitionSplitReader(
		Properties props,
		KafkaDeserializer<T> deserializationSchema) {
		Properties consumerProps = new Properties();
		consumerProps.putAll(props);
		consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, createConsumerClientId(props));
		this.consumer = new KafkaConsumer<>(consumerProps);
		this.stoppingOffsets = new HashMap<>();
		this.deserializationSchema = deserializationSchema;
		this.collector = new SimpleCollector<>();
		this.groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
	}

	@Override
	public RecordsWithSplitIds<Tuple3<T, Long, Long>> fetch() throws IOException {
		KafkaPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits = new KafkaPartitionSplitRecords<>();
		ConsumerRecords<byte[], byte[]> consumerRecords;
		try {
			consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
		} catch (WakeupException we) {
			return recordsBySplits;
		}

		List<TopicPartition> finishedPartitions = new ArrayList<>();
		for (TopicPartition tp : consumerRecords.partitions()) {
			long stoppingOffset = stoppingOffsets.getOrDefault(tp, Long.MAX_VALUE);
			String splitId = tp.toString();
			Collection<Tuple3<T, Long, Long>> recordsForSplit = recordsBySplits.recordsForSplit(splitId);
			for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords.records(tp)) {
				// Stop consuming from this partition if the offsets has reached the stopping offset.
				// Note that there are two cases, either case finishes a split:
				// 1. After processing a record with offset of "stoppingOffset - 1". The split reader
				//    should not continue fetching because the record with stoppingOffset may not exist.
				// 2. Before processing a record whose offset is greater than or equals to the stopping
				//    offset. This should only happens when case 1 was not met due to log compaction or
				//    log retention.
				// Case 2 is handled here. Case 1 is handled after the record is processed.
				if (consumerRecord.offset() >= stoppingOffset) {
					finishSplitAtRecord(tp, stoppingOffset, consumerRecord.offset(),
						finishedPartitions, recordsBySplits);
					break;
				}
				// Add the record to the partition collector.
				try {
					deserializationSchema.deserialize(consumerRecord, collector);
					collector.getRecords().forEach(r ->
						recordsForSplit.add(new Tuple3<>(r,
							consumerRecord.offset(),
							consumerRecord.timestamp())));
					// Finish the split because there might not be any message after this point. Keep polling
					// will just block forever.
					if (consumerRecord.offset() == stoppingOffset - 1) {
						finishSplitAtRecord(tp, stoppingOffset, consumerRecord.offset(),
							finishedPartitions, recordsBySplits);
					}
				} catch (Exception e) {
					throw new IOException("Failed to deserialize consumer record due to", e);
				} finally {
					collector.reset();
				}
			}
		}
		// Unassign the partitions that has finished.
		if (!finishedPartitions.isEmpty()) {
			unassignPartition(finishedPartitions);
		}
		return recordsBySplits;
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<KafkaPartitionSplit>> splitsChanges) {
		SplitsChange<KafkaPartitionSplit> splitChange;
		// Assignment.
		List<TopicPartition> newPartitionAssignments = new ArrayList<>();
		// Starting offsets
		Map<TopicPartition, Long> offsetsToSeek = new HashMap<>();
		List<TopicPartition> partitionsSeekToBeginning = new ArrayList<>();
		List<TopicPartition> partitionsSeekToEnd = new ArrayList<>();
		// Stopping offsets
		Set<TopicPartition> stopAtEnd = new HashSet<>();
		Set<TopicPartition> stopAtCommitted = new HashSet<>();

		// Get all the partition assignments and stopping offsets.
		while ((splitChange = splitsChanges.poll()) != null) {
			if (!(splitChange instanceof SplitsAddition)) {
				throw new UnsupportedOperationException(String.format(
					"The SplitChange type of %s is not supported.", splitChange.getClass()));
			}
			LOG.debug("Handling split change {}", splitChange);
			splitChange.splits().forEach(s -> {
				TopicPartition tp = s.getTopicPartition();
				newPartitionAssignments.add(tp);

				// Parse starting offsets.
				if (s.getStartingOffset() == KafkaPartitionSplit.EARLIEST_OFFSET) {
					partitionsSeekToBeginning.add(tp);
				} else if (s.getStartingOffset() == KafkaPartitionSplit.LATEST_OFFSET) {
					partitionsSeekToEnd.add(tp);
				} else if (s.getStartingOffset() == KafkaPartitionSplit.COMMITTED_OFFSET) {
					// Do nothing here, the consumer will first try to get the committed offsets of
					// these partitions by default.
				} else {
					offsetsToSeek.put(tp, s.getStartingOffset());
				}

				// Parse stopping offsets
				s.getStoppingOffset().ifPresent(stoppingOffset -> {
					if (stoppingOffset >= 0) {
						stoppingOffsets.put(tp, stoppingOffset);
					} else if (stoppingOffset == KafkaPartitionSplit.LATEST_OFFSET) {
						stopAtEnd.add(tp);
					} else if (stoppingOffset == KafkaPartitionSplit.COMMITTED_OFFSET) {
						stopAtCommitted.add(tp);
					} else {
						// This should not happen.
						throw new FlinkRuntimeException(String.format(
							"Invalid stopping offset %d for partition %s", stoppingOffset, tp));
					}
				});
			});
		}

		// Assign new partitions.
		LOG.debug("Assigning new partitions {}", newPartitionAssignments);
		newPartitionAssignments.addAll(consumer.assignment());
		consumer.assign(newPartitionAssignments);

		// Seek on the newly assigned partitions to their stating offsets.
		LOG.debug("Seeking to offsets: {}", offsetsToSeek);
		LOG.debug("Seeking to beginning: {}", partitionsSeekToBeginning);
		LOG.debug("Seeking to end: {}", partitionsSeekToEnd);

		offsetsToSeek.forEach(consumer::seek);
		if (!partitionsSeekToBeginning.isEmpty()) {
			consumer.seekToBeginning(partitionsSeekToBeginning);
		}
		if (!partitionsSeekToEnd.isEmpty()) {
			consumer.seekToEnd(partitionsSeekToEnd);
		}

		// Set the stopping offsets
		Map<TopicPartition, Long> endOffset = consumer.endOffsets(stopAtEnd);
		stoppingOffsets.putAll(endOffset);
		// TODO: Use batch committed offsets query interface after upgrading to Kafka 2.4.0+.
		stopAtCommitted.forEach(tp -> {
			OffsetAndMetadata oam = consumer.committed(tp);
			Preconditions.checkNotNull(oam, String.format("Partition %s should stop at committed offset. " +
				"But there is no committed offset of this partition for group %s", tp, groupId));
			stoppingOffsets.put(tp, oam.offset());
		});
	}

	@Override
	public void wakeUp() {
		consumer.wakeup();
	}

	// --------------- private helper method ----------------------

	private Future<Void> unassignPartition(Collection<TopicPartition> partitionsToUnassign) {
		Collection<TopicPartition> newAssignment = new HashSet<>(consumer.assignment());
		newAssignment.removeAll(partitionsToUnassign);
		consumer.assign(newAssignment);
		return newAssignment.isEmpty() ? new CompletableFuture<>() : READY;
	}

	private String createConsumerClientId(Properties props) {
		String prefix = props.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
		return prefix + "-" + new Random().nextLong();
	}

	private void finishSplitAtRecord(
		TopicPartition tp,
		long stoppingOffset,
		long currentOffset,
		List<TopicPartition> finishedPartitions,
		KafkaPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits) {
		LOG.debug("{} has reached stopping offset {}, current offset is {}", tp, stoppingOffset, currentOffset);
		finishedPartitions.add(tp);
		recordsBySplits.addFinishedSplit(KafkaPartitionSplit.toSplitId(tp));
	}

	// ---------------- private helper class ------------------------

	private static class KafkaPartitionSplitRecords<T> implements RecordsWithSplitIds<T> {
		private final Map<String, Collection<T>> recordsBySplits;
		private final Set<String> finishedSplits;

		private KafkaPartitionSplitRecords() {
			this.recordsBySplits = new HashMap<>();
			this.finishedSplits = new HashSet<>();
		}

		private Collection<T> recordsForSplit(String splitId) {
			return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
		}

		private void addFinishedSplit(String splitId) {
			finishedSplits.add(splitId);
		}

		@Override
		public Collection<String> splitIds() {
			return recordsBySplits.keySet();
		}

		@Override
		public Map<String, Collection<T>> recordsBySplits() {
			return recordsBySplits;
		}

		@Override
		public Set<String> finishedSplits() {
			return finishedSplits;
		}
	}

	private static class SimpleCollector<T> implements Collector<T> {
		private final List<T> records = new ArrayList<>();

		@Override
		public void collect(T record) {
			records.add(record);
		}

		@Override
		public void close() {

		}

		private List<T> getRecords() {
			return records;
		}

		private void reset() {
			records.clear();
		}
	}

}
