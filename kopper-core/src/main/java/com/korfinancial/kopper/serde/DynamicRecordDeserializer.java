/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde;

import java.lang.reflect.Proxy;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.korfinancial.kopper.dyre.GenericRecordInvocationHandler;

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
