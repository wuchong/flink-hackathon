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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplitState;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * The source reader for Kafka partitions.
 */
public class KafkaSourceReader<T>
		extends SingleThreadMultiplexSourceReaderBase<Tuple3<T, Long, Long>, T, KafkaPartitionSplit, KafkaPartitionSplitState> {

	public KafkaSourceReader(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
			Supplier<SplitReader<Tuple3<T, Long, Long>, KafkaPartitionSplit>> splitReaderSupplier,
			RecordEmitter<Tuple3<T, Long, Long>, T, KafkaPartitionSplitState> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		super(futureNotifier, elementsQueue, splitReaderSupplier, recordEmitter, config, context);
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {

	}

	@Override
	protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
		return new KafkaPartitionSplitState(split);
	}

	@Override
	protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
		return splitState.toKafkaPartitionSplit();
	}
}
