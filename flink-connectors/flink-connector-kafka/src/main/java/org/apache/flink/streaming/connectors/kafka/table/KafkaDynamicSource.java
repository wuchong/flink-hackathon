/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.kafka.source.KafkaSource;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.reader.deserializer.DeserializationSchemaWrapper;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka {@link org.apache.flink.table.connector.source.DynamicTableSource}.
 */
@Internal
public class KafkaDynamicSource implements ScanTableSource {

	// --------------------------------------------------------------------------------------------
	// Common attributes
	// --------------------------------------------------------------------------------------------
	protected final TableSchema physicalTableSchema;

	// --------------------------------------------------------------------------------------------
	// Scan format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topic to consume. */
	protected final String topic;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	protected final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	protected final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	protected final long startupTimestampMillis;

	/**
	 * The index of Kafka message timestamp in the schema.
	 */
	protected final int timestampIndex;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param tableSchema	         Source output schema
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 */
	public KafkaDynamicSource(
			TableSchema tableSchema,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			int timestampIndex) {

		this.physicalTableSchema = Preconditions.checkNotNull(
			tableSchema, "Produced data type must not be null.");
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.decodingFormat = Preconditions.checkNotNull(
			decodingFormat, "Decoding format must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.startupTimestampMillis = startupTimestampMillis;
		this.timestampIndex = timestampIndex;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return this.decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		KafkaSource<RowData> kafkaSource = KafkaSource.<RowData>builder()
			.setTopics(Collections.singletonList(topic))
			.setDeserializer(getDeserializer(runtimeProviderContext))
			.setClientIdPrefix(UUID.randomUUID().toString())
			.setProperties(properties)
			.setStartingOffsetInitializer(getOffsetInitializer())
			.build();
		return SourceProvider.of(kafkaSource);
	}

	private KafkaDeserializer<RowData> getDeserializer(ScanContext runtimeProviderContext) {
		if (timestampIndex > 0) {
			int arity = physicalTableSchema.getFieldCount();
			int[] projection = new int[arity - 1];
			for (int i = 0; i < arity; i++) {
				if (i < timestampIndex) {
					projection[i] = i;
				} else if (i > timestampIndex) {
					projection[i - 1] = i;
				}
			}
			TableSchema formatSchema = TableSchemaUtils.projectSchema(physicalTableSchema, projection);
			DeserializationSchema<RowData> deserializationSchema =
				this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, formatSchema.toRowDataType());
			return new KafkaTableDeserializer(deserializationSchema, timestampIndex);
		} else {
			DeserializationSchema<RowData> deserializationSchema =
				this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.physicalTableSchema.toRowDataType());
			return new DeserializationSchemaWrapper<>(deserializationSchema);
		}
	}

	private OffsetsInitializer getOffsetInitializer() {
		switch (startupMode) {
			case EARLIEST:
				return OffsetsInitializer.earliest();
			case LATEST:
				return OffsetsInitializer.latest();
			case GROUP_OFFSETS:
				return OffsetsInitializer.committedOffsets();
			case SPECIFIC_OFFSETS:
				Map<TopicPartition, Long> offsets = new HashMap<>();
				specificStartupOffsets.forEach((p, offset) -> {
					offsets.put(new TopicPartition(p.getTopic(), p.getPartition()), offset);
				});
				return OffsetsInitializer.offsets(offsets);
			case TIMESTAMP:
				return OffsetsInitializer.timestamps(startupTimestampMillis);
			default:
				throw new UnsupportedOperationException("Unsupported startup mode: " + startupMode);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaDynamicSource that = (KafkaDynamicSource) o;
		return Objects.equals(physicalTableSchema, that.physicalTableSchema) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			physicalTableSchema,
			topic,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	@Override
	public DynamicTableSource copy() {
		return new KafkaDynamicSource(
				this.physicalTableSchema,
				this.topic,
				this.properties,
				this.decodingFormat,
				this.startupMode,
				this.specificStartupOffsets,
				this.startupTimestampMillis,
				this.timestampIndex);
	}

	@Override
	public String asSummaryString() {
		return "Kafka";
	}
}
