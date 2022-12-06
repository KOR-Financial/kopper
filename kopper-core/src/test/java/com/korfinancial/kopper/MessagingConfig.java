/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
