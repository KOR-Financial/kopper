/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

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
