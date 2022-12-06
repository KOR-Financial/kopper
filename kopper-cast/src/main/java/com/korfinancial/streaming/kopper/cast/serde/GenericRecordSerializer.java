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
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author Daan Gerits
 */
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
