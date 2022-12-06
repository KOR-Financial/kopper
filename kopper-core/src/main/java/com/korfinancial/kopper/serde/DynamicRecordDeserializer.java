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

package com.korfinancial.kopper.serde;

import java.lang.reflect.Proxy;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.korfinancial.kopper.dyre.GenericRecordInvocationHandler;

/**
 * @author Daan Gerits
 */
public class DynamicRecordDeserializer<T> implements Deserializer<T> {

	private final Class<T> cls;

	private final KafkaAvroDeserializer inner;

	public DynamicRecordDeserializer(final Class<T> cls) {
		this.cls = cls;
		inner = new KafkaAvroDeserializer();
	}

	DynamicRecordDeserializer(final Class<T> cls, final SchemaRegistryClient client) {
		this.cls = cls;
		inner = new KafkaAvroDeserializer(client);
	}

	@Override
	public void configure(final Map<String, ?> deserializerConfig, final boolean isDeserializerForRecordKeys) {
		inner.configure(deserializerConfig, isDeserializerForRecordKeys);
	}

	@Override
	public T deserialize(final String topic, final byte[] bytes) {
		GenericRecord gr = (GenericRecord) inner.deserialize(topic, bytes);
		return (T) Proxy.newProxyInstance(cls.getClassLoader(), new Class[] { cls },
				new GenericRecordInvocationHandler(gr));
	}

	@Override
	public void close() {
		inner.close();
	}

}
