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

package org.apache.flink.connectors.kafka.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.connector.source.hybrid.SwitchableSource;
import org.apache.flink.api.connector.source.hybrid.SwitchableSplitEnumerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connectors.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connectors.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connectors.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connectors.kafka.source.enumerator.KafkaSwitchableSourceEnumerator;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connectors.kafka.source.reader.KafkaPartitionSplitReader;
import org.apache.flink.connectors.kafka.source.reader.KafkaRecordEmitter;
import org.apache.flink.connectors.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * The Source implementation of Kafka.
 *
 * @param <OUT> the output type of the source.
 */
public class KafkaSwitchableSource<OUT> extends KafkaSource<OUT>
	implements SwitchableSource<OUT, KafkaPartitionSplit, KafkaSourceEnumState, Long, Void> {

	KafkaSwitchableSource(KafkaSubscriber subscriber, OffsetsInitializer startingOffsetsInitializer, @Nullable OffsetsInitializer stoppingOffsetsInitializer, Boundedness boundedness, KafkaDeserializer<OUT> deserializationSchema, Properties props) {
		super(subscriber, startingOffsetsInitializer, stoppingOffsetsInitializer, boundedness, deserializationSchema, props);
	}

	/**
	 * Get a kafkaSwitchableSourceBuilderto build a {@link KafkaSource}.
	 *
	 * @return a Kafka source builder.
	 */
	public static <OUT> KafkaSwitchableSourceBuilder<OUT> builder() {
		return new KafkaSwitchableSourceBuilder<>();
	}

	@Override
	public SwitchableSplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState, Long, Void> createEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
		return new KafkaSwitchableSourceEnumerator(
			subscriber,
			startingOffsetsInitializer,
			stoppingOffsetsInitializer,
			props,
			enumContext);
	}

	@Override
	public SwitchableSplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState, Long, Void> restoreEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint) throws IOException {
		return new KafkaSwitchableSourceEnumerator(
			subscriber,
			startingOffsetsInitializer,
			stoppingOffsetsInitializer,
			props,
			enumContext,
			checkpoint.getCurrentAssignment());
	}
}
