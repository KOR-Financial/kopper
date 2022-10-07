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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

/**
 * @author Daan Gerits
 */
public abstract class CastSerdes {

	public static Serde<GenericRecord> upcastingSerde(UpcasterRegistry upcasterRegistry) {
		return Serdes.serdeFrom(new GenericRecordSerializer(), new UpcastingDeserializer(upcasterRegistry));
	}

	public static Serde<GenericRecord> upcastingSerde(UpcasterRegistry upcasterRegistry,
			SchemaRegistryClient schemaRegistryClient) {
		return Serdes.serdeFrom(new GenericRecordSerializer(schemaRegistryClient),
				new UpcastingDeserializer(upcasterRegistry, schemaRegistryClient));
	}

}
