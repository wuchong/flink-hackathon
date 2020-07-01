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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class KafkaTableDeserializer implements KafkaDeserializer<RowData> {
	private static final long serialVersionUID = 6904523577274548486L;

	private final DeserializationSchema<RowData> deserializationSchema;
	private final int arity;
	private final int timestampIndex;

	public KafkaTableDeserializer(DeserializationSchema<RowData> deserializationSchema, int timestampIndex) {
		this.deserializationSchema = deserializationSchema;
		this.timestampIndex = timestampIndex;
		this.arity = deserializationSchema.getProducedType().getArity() + 1;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		deserializationSchema.open(context);
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) throws Exception {
		long ts = record.timestamp();
		List<RowData> values = new ArrayList<>();
		Collector<RowData> listCollector = new ListCollector<>(values);
		deserializationSchema.deserialize(record.value(), listCollector);
		for (RowData value : values) {
			GenericRowData v = (GenericRowData) value;
			GenericRowData result = new GenericRowData(arity);
			result.setRowKind(v.getRowKind());
			for (int i = 0; i < arity; i++) {
				if (i < timestampIndex) {
					result.setField(i, v.getField(i));
				} else if (i == timestampIndex) {
					result.setField(i, TimestampData.fromEpochMillis(ts));
				} else {
					result.setField(i, v.getField(i - 1));
				}
			}
			collector.collect(result);
		}
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		// TODO: should add the timestamp type, update this in the future,
		//  because the returned value is never been used.
		return deserializationSchema.getProducedType();
	}
}
