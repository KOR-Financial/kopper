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

package com.korfinancial.streaming.kopper.cast.serde;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.korfinancial.streaming.kopper.cast.DeclarativeUpcasterChain;
import com.korfinancial.streaming.kopper.cast.DeclarativeUpcasterContext;
import com.korfinancial.streaming.kopper.cast.VersionedItem;
import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

/**
 * @author Daan Gerits
 */
public class UpcastingDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<GenericRecord> {

	private final UpcasterRegistry<DeclarativeUpcasterContext, DeclarativeUpcasterChain<GenericRecord>> upcasterRegistry;

	private boolean isKey;

	public UpcastingDeserializer(
			final UpcasterRegistry<DeclarativeUpcasterContext, DeclarativeUpcasterChain<GenericRecord>> upcasterRegistry) {
		this.upcasterRegistry = upcasterRegistry;
	}

	UpcastingDeserializer(
			final UpcasterRegistry<DeclarativeUpcasterContext, DeclarativeUpcasterChain<GenericRecord>> upcasterRegistry,
			final SchemaRegistryClient client) {
		schemaRegistry = client;
		this.upcasterRegistry = upcasterRegistry;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		configure(new KafkaAvroDeserializerConfig(configs));
	}

	@Override
	public GenericRecord deserialize(final String topic, final byte[] bytes) {
		GenericContainerWithVersion gr = deserializeWithSchemaAndVersion(topic, isKey, bytes);

		if (gr.container().getSchema().getType() != Schema.Type.RECORD) {
			throw new RuntimeException("Only records are supported as the root elements.");
		}

		DeclarativeUpcasterChain<GenericRecord> chain = upcasterRegistry.getUpcasters(topic + "-value");
		if (chain == null) {
			// -- no upcasters available, so we will just return what we got from the wire
			return (GenericRecord) gr.container();
		}

		VersionedItem<GenericRecord> result = chain
				.doUpcast(new VersionedItem<>((GenericRecord) gr.container(), gr.version()));
		return result.getItem();
	}

}
