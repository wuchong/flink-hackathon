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

package org.apache.flink.connectors.kafka.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A simple wrapper for using the {@link DeserializationSchema} as {@link KafkaDeserializer}.
 */
public class DeserializationSchemaWrapper<T> implements KafkaDeserializer<T> {
	private static final long serialVersionUID = 5738490926777597728L;

	private final DeserializationSchema<T> deserializationSchema;

	public DeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		deserializationSchema.open(context);
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> collector) throws Exception {
		deserializationSchema.deserialize(record.value(), collector);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
