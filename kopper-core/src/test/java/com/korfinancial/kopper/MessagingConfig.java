/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper;

import java.util.Map;
import java.util.UUID;

import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;

public abstract class MessagingConfig {

	public static Map<String, Object> producerConfig(EmbeddedKafkaBroker embeddedKafkaBroker) {
		Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		props.put("schema.registry.url", "schema.registry.url");
		props.put("avro.use.logical.type.converters", "true");
		props.put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
		return props;
	}

	public static Map<String, Object> consumerConfig(EmbeddedKafkaBroker embeddedKafkaBroker) {
		Map<String, Object> props = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false",
				embeddedKafkaBroker);
		props.put("schema.registry.url", "schema.registry.url");
		props.put("avro.use.logical.type.converters", "true");
		props.put("spring.json.trusted.packages", "com.korfinancial.*");
		props.put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

		return props;
	}

}
