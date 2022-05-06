/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.korfinancial.kopper.dyre.DynamicRecord;

public abstract class KopperSerdes {

	public static <T extends DynamicRecord> Serde<T> dynamicSerde(Class<T> cls) {
		return Serdes.serdeFrom(dynamicSerializer(), dynamicDeserializer(cls));
	}

	public static <T extends DynamicRecord> Serde<T> dynamicSerde(Class<T> cls,
			SchemaRegistryClient schemaRegistryClient) {
		return Serdes.serdeFrom(dynamicSerializer(schemaRegistryClient),
				dynamicDeserializer(cls, schemaRegistryClient));
	}

	public static <T extends DynamicRecord> Serializer<T> dynamicSerializer() {
		return new DynamicRecordSerializer<>();
	}

	public static <T extends DynamicRecord> Serializer<T> dynamicSerializer(SchemaRegistryClient schemaRegistryClient) {
		return new DynamicRecordSerializer<>(schemaRegistryClient);
	}

	public static <T extends DynamicRecord> Deserializer<T> dynamicDeserializer(Class<T> cls) {
		return new DynamicRecordDeserializer<>(cls);
	}

	public static <T extends DynamicRecord> Deserializer<T> dynamicDeserializer(Class<T> cls,
			SchemaRegistryClient schemaRegistryClient) {
		return new DynamicRecordDeserializer<>(cls, schemaRegistryClient);
	}

}
