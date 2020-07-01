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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connectors.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connectors.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connectors.kafka.source.reader.deserializer.KafkaDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base builder class for {@link KafkaSource} to make it easier for the users to construct
 * a Kafka source.
 */
public class KafkaSourceBuilder<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBuilder.class);
	private static final String[] REQUIRED_CONFIGS = {ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG};
	// The subscriber specifies the partitions to subscribe to.
	private KafkaSubscriber subscriber;
	// Users can specify the starting / stopping offset initializer.
	private OffsetsInitializer startingOffsetsInitializer;
	private OffsetsInitializer stoppingOffsetsInitializer;
	// Boundedness
	private Boundedness boundedness;
	private KafkaDeserializer<OUT> deserializationSchema;
	// The configurations.
	protected Properties props;

	KafkaSourceBuilder() {
		this.subscriber = null;
		this.startingOffsetsInitializer = OffsetsInitializer.earliest();
		this.stoppingOffsetsInitializer = null;
		this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
		this.deserializationSchema = null;
		this.props = new Properties();
	}

	public KafkaSourceBuilder<OUT> setTopics(List<String> topics) {
		ensureSubscriberIsNull("topics");
		subscriber = KafkaSubscriber.getTopicListSubscriber(topics);
		return this;
	}

	public KafkaSourceBuilder<OUT> setTopicPattern(Pattern topicPattern) {
		ensureSubscriberIsNull("topic pattern");
		subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);
		return this;
	}

	public KafkaSourceBuilder<OUT> setPartitions(Set<TopicPartition> partitions) {
		ensureSubscriberIsNull("partitions");
		subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);
		return this;
	}

	public KafkaSourceBuilder<OUT> setStartingOffsetInitializer(OffsetsInitializer startingOffsetsInitializer) {
		this.startingOffsetsInitializer = startingOffsetsInitializer;
		return this;
	}

	public KafkaSourceBuilder<OUT> setUnbounded() {
		this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
		this.stoppingOffsetsInitializer = null;
		return this;
	}

	public KafkaSourceBuilder<OUT> setUnbounded(OffsetsInitializer stoppingOffsetsInitializer) {
		this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
		this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
		return this;
	}

	public KafkaSourceBuilder<OUT> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
		this.boundedness = Boundedness.BOUNDED;
		this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
		return this;
	}

	public KafkaSourceBuilder<OUT> setDeserializer(KafkaDeserializer<OUT> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
		return this;
	}

	public KafkaSourceBuilder<OUT> setClientIdPrefix(String prefix) {
		return setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
	}

	public KafkaSourceBuilder<OUT> setProperty(String key, String value) {
		props.setProperty(key, value);
		return this;
	}

	public KafkaSourceBuilder<OUT> setProperties(Properties props) {
		this.props.putAll(props);
		return this;
	}

	public KafkaSource<OUT> build() {
		sanityCheck();
		parseAndSetRequiredProperties();
		return new KafkaSource<>(
			subscriber,
			startingOffsetsInitializer,
			stoppingOffsetsInitializer,
			boundedness,
			deserializationSchema,
			props);
	}

	// ------------- private helpers  --------------

	private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
		if (subscriber != null) {
			throw new IllegalStateException(String.format(
				"Cannot use %s for consumption because a %s is already set for consumption.",
				attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
		}
	}

	private void parseAndSetRequiredProperties() {
		maybeOverride(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(), true);
		maybeOverride(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(), true);
		maybeOverride(ConsumerConfig.GROUP_ID_CONFIG, "KafkaSource-" + new Random().nextLong(), false);
		maybeOverride(
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
			startingOffsetsInitializer.getAutoOffsetResetStrategy().name().toLowerCase(),
			true);

		// Sets stopping offset initializer.
		if (stoppingOffsetsInitializer == null) {
			stoppingOffsetsInitializer = OffsetsInitializer.noStoppingOffsets();
		}

		// If the source is bounded, do not run periodic partition discovery.
		if (maybeOverride(
			KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
			"-1",
			boundedness == Boundedness.BOUNDED)) {
			LOG.warn("{} property is overridden to -1 because the source is bounded.",
				KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
		}

		// If the client id prefix is not set, reuse the consumer group id as the client id prefix.
		maybeOverride(
			KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
			props.getProperty(ConsumerConfig.GROUP_ID_CONFIG),
			false);
	}

	private boolean maybeOverride(String key, String value, boolean override) {
		boolean overridden = false;
		String userValue = props.getProperty(key);
		if (userValue != null) {
			if (override) {
				LOG.warn(String.format("Property %s is provided but will be overridden from %s to %s",
					key, userValue, value));
				props.setProperty(key, value);
				overridden = true;
			}
		} else {
			props.setProperty(key, value);
		}
		return overridden;
	}

	private void sanityCheck() {
		// Check required configs.
		for (String requiredConfig : REQUIRED_CONFIGS) {
			checkNotNull(props.getProperty(
				requiredConfig,
				String.format("Property %s is required but not provided", requiredConfig)));
		}
		// Check required settings.
		checkNotNull(subscriber, "No subscribe mode is specified, " +
			"should be one of topics, topic pattern and partition set.");
		checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");
	}
}
