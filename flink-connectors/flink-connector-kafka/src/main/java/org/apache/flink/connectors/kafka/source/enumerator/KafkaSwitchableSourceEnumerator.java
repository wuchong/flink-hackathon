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

package org.apache.flink.connectors.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.hybrid.SwitchableSplitEnumerator;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connectors.kafka.source.split.KafkaPartitionSplit;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The enumerator class for Kafka source.
 */
@Internal
public class KafkaSwitchableSourceEnumerator extends KafkaSourceEnumerator
	implements SwitchableSplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState, Long, Void> {

	public KafkaSwitchableSourceEnumerator(KafkaSubscriber subscriber, OffsetsInitializer startingOffsetInitializer,
										   OffsetsInitializer stoppingOffsetInitializer, Properties properties, SplitEnumeratorContext<KafkaPartitionSplit> context) {
		super(subscriber, startingOffsetInitializer, stoppingOffsetInitializer, properties, context);
	}

	public KafkaSwitchableSourceEnumerator(KafkaSubscriber subscriber, OffsetsInitializer startingOffsetInitializer,
										   OffsetsInitializer stoppingOffsetInitializer, Properties properties, SplitEnumeratorContext<KafkaPartitionSplit> context, Map<Integer, Set<KafkaPartitionSplit>> currentSplitsAssignments) {
		super(subscriber, startingOffsetInitializer, stoppingOffsetInitializer, properties, context, currentSplitsAssignments);
	}

	@Override
	public void setStartState(Long startState) {
		this.startingOffsetInitializer = OffsetsInitializer.timestamps(startState);
	}

	@Override
	public Void getEndState() {
		return null;
	}
}
