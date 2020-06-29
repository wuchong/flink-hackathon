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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SourceReaderTestBase;
import org.apache.flink.connectors.kafka.source.KafkaSource;
import org.apache.flink.connectors.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link KafkaSourceReader}.
 */
public class KafkaSourceReaderTest extends SourceReaderTestBase<KafkaPartitionSplit> {
	private static final String TOPIC = "KafkaSourceReaderTest";
	private static final String WATERMARK_TEST_TOPIC = "KafkaSourceReaderTest-Watermark";

	@BeforeClass
	public static void setup() throws Throwable {
		KafkaSourceTestEnv.setup();
		try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
			adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC, NUM_SPLITS, (short) 1)));
		}
		KafkaSourceTestEnv.produceToKafka(getRecords(), StringSerializer.class, IntegerSerializer.class);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		KafkaSourceTestEnv.tearDown();
	}

	// ------------------------------------------

	@Override
	protected SourceReader<Integer, KafkaPartitionSplit> createReader() {
		KafkaSource<Integer> source = KafkaSource
				.<Integer>builder()
				.setBounded(OffsetsInitializer.latest())
				.setClientIdPrefix("KafkaSourceReaderTest")
				.setDeserializer(KafkaDeserializer.valueOnly(IntegerDeserializer.class))
				.setPartitions(Collections.singleton(new TopicPartition("AnyTopic", 0)))
				.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSourceTestEnv.brokerConnectionStrings)
				.build();
		return source.createReader(new SourceReaderContext() {
			@Override
			public MetricGroup metricGroup() {
				return new UnregisteredMetricsGroup();
			}

			@Override
			public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {

			}
		});
	}

	@Override
	protected List<KafkaPartitionSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<KafkaPartitionSplit> splits = new ArrayList<>();
		for (int i = 0; i < numRecordsPerSplit; i++) {
			splits.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return splits;
	}

	@Override
	protected KafkaPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
		long stoppingOffset =
				boundedness == Boundedness.BOUNDED ? NUM_RECORDS_PER_SPLIT : KafkaPartitionSplit.NO_STOPPING_OFFSET;
		return new KafkaPartitionSplit(
				new TopicPartition(TOPIC, splitId),
				0L,
				stoppingOffset);
	}

	@Override
	protected long getNextRecordIndex(KafkaPartitionSplit split) {
		return split.getStartingOffset();
	}

	// ---------------------

	private static List<ProducerRecord<String, Integer>> getRecords() {
		List<ProducerRecord<String, Integer>> records = new ArrayList<>();
		for (int part = 0; part < NUM_SPLITS; part++) {
			for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
				records.add(new ProducerRecord<>(
						TOPIC,
						part,
						TOPIC + "-" + part,
						part * NUM_RECORDS_PER_SPLIT + i));
			}
		}
		return records;
	}
}
