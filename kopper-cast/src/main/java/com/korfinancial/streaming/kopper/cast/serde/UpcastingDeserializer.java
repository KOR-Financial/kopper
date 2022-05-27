/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
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

import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.VersionedItem;
import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

public class UpcastingDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<GenericRecord> {

	private final UpcasterRegistry upcasterRegistry;

	private boolean isKey;

	public UpcastingDeserializer(final UpcasterRegistry upcasterRegistry) {
		this.upcasterRegistry = upcasterRegistry;
	}

	UpcastingDeserializer(final UpcasterRegistry upcasterRegistry, final SchemaRegistryClient client) {
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

		UpcasterChain<GenericRecord> chain = upcasterRegistry.getUpcasters(topic + "-value");
		if (chain == null) {
			// -- no upcasters available, so we will just return what we got from the wire
			return (GenericRecord) gr.container();
		}

		VersionedItem<GenericRecord> result = chain
				.doUpcast(new VersionedItem<>((GenericRecord) gr.container(), gr.version()));
		return result.getItem();
	}

}
