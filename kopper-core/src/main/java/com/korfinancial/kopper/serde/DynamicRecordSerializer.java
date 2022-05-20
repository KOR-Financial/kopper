/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.korfinancial.kopper.dyre.DynamicRecord;

public class DynamicRecordSerializer<T extends DynamicRecord> implements Serializer<T> {

	private final KafkaAvroSerializer inner;

	public DynamicRecordSerializer() {
		this.inner = new KafkaAvroSerializer();
	}

	DynamicRecordSerializer(final SchemaRegistryClient client) {
		inner = new KafkaAvroSerializer(client);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		inner.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, T data) {
		GenericRecord record = null;
		if (data != null) {
			record = data.record();
		}

		try {
			return inner.serialize(topic, record);
		}
		catch (SerializationException se) {
			if (record != null) {
				throw new SerializationException("Unable to serialize " + record.getSchema().getFullName(), se);
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
