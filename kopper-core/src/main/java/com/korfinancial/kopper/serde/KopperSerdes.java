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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.korfinancial.kopper.dyre.DynamicRecord;

/**
 * @author Daan Gerits
 */
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
