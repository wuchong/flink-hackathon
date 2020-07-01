/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.hybrid;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.hybrid.HybridSource;
import org.apache.flink.api.connector.source.hybrid.HybridSourceBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSwitchableSource;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connectors.kafka.source.KafkaSwitchableSource;
import org.apache.flink.connectors.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.examples.statemachine.StateMachineExample;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.Serializable;
import java.util.*;

/**
 * Job to generate input events that are written to Kafka, for the {@link StateMachineExample} job.
 */
public class HybridSourceDemo {

	public static void main(String[] args) throws Exception {
		FileSwitchableSource<PartitionAndValue> fileSource = new FileSwitchableSource<>(new Path(""));
		KafkaSwitchableSource<PartitionAndValue> kafkaSource = KafkaSwitchableSource
			.<PartitionAndValue>builder()
			.setClientIdPrefix("hybrid-kafka-source")
			.setTopics(Collections.singletonList("topic1"))
			.setDeserializer(new HybridKafkaDeserializer())
			.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "")
			.setStartingOffsetInitializer(OffsetsInitializer.earliest())
			.setBounded(OffsetsInitializer.latest())
			.buildSwitchable();
		HybridSource<PartitionAndValue, FileSourceSplit, KafkaPartitionSplit, PendingSplitsCheckpoint, KafkaSourceEnumState, Long> hybridSource =
			new HybridSourceBuilder<PartitionAndValue, FileSourceSplit, KafkaPartitionSplit, PendingSplitsCheckpoint, KafkaSourceEnumState, Long>()
				.addFirstSource(fileSource)
				.addSecondSource(kafkaSource).build();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<PartitionAndValue> stream = env.fromSource(
			hybridSource,
			WatermarkStrategy.noWatermarks(),
			"hybrid-source",
			TypeInformation.of(PartitionAndValue.class));
		executeAndVerify(env, stream);
	}

	private static class PartitionAndValue implements Serializable {
		private static final long serialVersionUID = 4813439951036021779L;
		private final String tp;
		private final int value;

		private PartitionAndValue(TopicPartition tp, int value) {
			this.tp = tp.toString();
			this.value = value;
		}

		public String getTp() {
			return tp;
		}

		public int getValue() {
			return value;
		}
	}

	private static class HybridKafkaDeserializer implements KafkaDeserializer<PartitionAndValue> {
		private static final long serialVersionUID = -3765473065594331694L;
		private transient Deserializer<Integer> deserializer;

		@Override
		public void deserialize(
			ConsumerRecord<byte[], byte[]> record,
			Collector<PartitionAndValue> collector) throws Exception {
			if (deserializer == null) {
				deserializer = new IntegerDeserializer();
			}
			collector.collect(new PartitionAndValue(
				new TopicPartition(record.topic(), record.partition()),
				deserializer.deserialize(record.topic(), record.value())));
		}

		@Override
		public TypeInformation<PartitionAndValue> getProducedType() {
			return TypeInformation.of(PartitionAndValue.class);
		}
	}

	private static void executeAndVerify(StreamExecutionEnvironment env, DataStream<PartitionAndValue> stream) throws Exception {
		stream.addSink(new RichSinkFunction<PartitionAndValue>() {
			@Override
			public void open(Configuration parameters) throws Exception {
				getRuntimeContext().addAccumulator("result", new ListAccumulator<PartitionAndValue>());
			}

			@Override
			public void invoke(PartitionAndValue value, Context context) throws Exception {
				getRuntimeContext().getAccumulator("result").add(value);
			}
		});
		List<PartitionAndValue> result = env.execute().getAccumulatorResult("result");
		Map<String, List<Integer>> resultPerPartition = new HashMap<>();
		result.forEach(pav -> resultPerPartition.computeIfAbsent(pav.tp, ignored -> new ArrayList<>()).add(pav.value));
		resultPerPartition.forEach((tp, values) -> {
			for (Integer value : values) {
				System.out.println(value);
			}
		});
	}
}
