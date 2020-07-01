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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TIMESTAMP_FIELD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableOptions;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSource}.
 */
public class KafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	public static final String IDENTIFIER = "kafka";

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(TOPIC);
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
			DeserializationFormatFactory.class,
			FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		validateTableOptions(tableOptions);

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		List<String> fieldNames = Arrays.asList(physicalSchema.getFieldNames());
		final KafkaOptions.StartupOptions startupOptions = getStartupOptions(tableOptions, topic);
		int timestampIndex = tableOptions.getOptional(KafkaOptions.TIMESTAMP_FIELD)
			.map(fieldNames::indexOf)
			.orElse(-1);
		physicalSchema.getFieldDataType(timestampIndex).ifPresent(dt -> {
			if (dt.getLogicalType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
				LogicalTypeChecks.getPrecision(dt.getLogicalType()) != 3) {
				throw new IllegalArgumentException("Kafka message timestamp field must be defined as TIMESTAMP(3).");
			}
		});
		return new KafkaDynamicSource(
			physicalSchema,
			topic,
			getKafkaProperties(context.getCatalogTable().getOptions()),
			decodingFormat,
			startupOptions.startupMode,
			startupOptions.specificOffsets,
			startupOptions.startupTimestampMillis,
			timestampIndex);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(TOPIC);
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		validateTableOptions(tableOptions);

		DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		return createKafkaTableSink(
			consumedDataType,
			topic,
			getKafkaProperties(context.getCatalogTable().getOptions()),
			getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()),
			encodingFormat);
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TOPIC);
		options.add(FactoryUtil.FORMAT);
		options.add(PROPS_BOOTSTRAP_SERVERS);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PROPS_GROUP_ID);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SINK_PARTITIONER);
		options.add(TIMESTAMP_FIELD);
		return options;
	}

	protected KafkaDynamicSinkBase createKafkaTableSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		return new KafkaDynamicSink(
				consumedDataType,
				topic,
				properties,
				partitioner,
				encodingFormat);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
