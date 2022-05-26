/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.serde;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class GenericRecordSerializer implements Serializer<GenericRecord> {

	private final KafkaAvroSerializer inner;

	public GenericRecordSerializer() {
		this.inner = new KafkaAvroSerializer();
	}

	GenericRecordSerializer(final SchemaRegistryClient client) {
		inner = new KafkaAvroSerializer(client);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		inner.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, GenericRecord data) {
		try {
			return inner.serialize(topic, data);
		}
		catch (SerializationException se) {
			if (data != null) {
				throw new SerializationException("Unable to serialize " + data.getSchema().getFullName(), se);
			}
			else {
				throw se;
			}
		}
	}

	@Override
	public void close() {
		inner.close();
	}

}
