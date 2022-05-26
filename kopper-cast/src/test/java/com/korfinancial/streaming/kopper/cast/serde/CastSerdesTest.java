/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.serde;

import java.util.Map;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.avro.AvroHelpers;
import com.korfinancial.streaming.kopper.cast.avro.DeclarativeAvroUpcaster;
import com.korfinancial.streaming.kopper.cast.avro.Payloads;
import com.korfinancial.streaming.kopper.cast.registry.InMemoryUpcasterRegistry;
import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;
import com.korfinancial.streaming.kopper.cast.sr.AvroUpcasterChain;

import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V1;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V2;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V3;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V4;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V5;
import static org.assertj.core.api.Assertions.assertThat;

class CastSerdesTest {

	public static final String TOPIC = "my.subject";

	public static final String SUBJECT = TOPIC + "-value";

	private Serializer<GenericRecord> serializer;

	private Deserializer<GenericRecord> deserializer;

	private static SchemaRegistryClient schemaRegistryClient;

	@BeforeAll
	static void beforeAll() throws Exception {
		schemaRegistryClient = new MockSchemaRegistryClient();
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V1), 1, 101);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V2), 2, 102);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V3), 3, 103);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V4), 4, 104);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V5), 5, 105);
	}

	@BeforeEach
	void setup() throws Exception {
		UpcasterRegistry upcasterRegistry = new InMemoryUpcasterRegistry();

		// @formatter-off
		upcasterRegistry.registerChain(AvroUpcasterChain.builder(SUBJECT)
				.register(DeclarativeAvroUpcaster.builder(schemaRegistryClient, SUBJECT, 3)
						.withExpression("age", "#input.age != null ? #parseInt(#input.age) : null")
						.withVariable("parseInt", Integer.class.getDeclaredMethod("parseInt", String.class)).build())
				.register(DeclarativeAvroUpcaster.builder(schemaRegistryClient, SUBJECT, 4)
						.withExpression("firstname", "#input.name?.split('\\s', 2)[0] ?: ''")
						.withExpression("lastname", "#input.name?.split('\\s', 2)[1] ?: ''").build())
				.register(DeclarativeAvroUpcaster.builder(schemaRegistryClient, SUBJECT, 5).withExpression("name",
						"#asRecord(#schema.getField('name').schema(), {firstname: #input.firstname, lastname: #input.lastname})")
						.withVariable("asRecord",
								AvroHelpers.class.getDeclaredMethod("asRecord", Schema.class, Map.class))
						.build())
				.build());
		// @formatter-on

		serializer = new GenericRecordSerializer(schemaRegistryClient);
		serializer.configure(Map.of("schema.registry.url", "mock://"), false);

		deserializer = new UpcastingDeserializer(upcasterRegistry, schemaRegistryClient);
		deserializer.configure(Map.of("schema.registry.url", "mock://"), false);
	}

	@Test
	void v1() {
		byte[] bytes = serializer.serialize(TOPIC, Payloads.RECORD_V1);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		GenericRecord latest = deserializer.deserialize(TOPIC, bytes);
		assertThat(latest).isEqualTo(Payloads.RECORD_V5_WITHOUT_AGE);
	}

	@Test
	void v2() {
		byte[] bytes = serializer.serialize(TOPIC, Payloads.RECORD_V2_WITH_AGE);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		GenericRecord latest = deserializer.deserialize(TOPIC, bytes);
		assertThat(latest).isEqualTo(Payloads.RECORD_V5);
	}

	@Test
	void v3() {
		byte[] bytes = serializer.serialize(TOPIC, Payloads.RECORD_V3);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		GenericRecord latest = deserializer.deserialize(TOPIC, bytes);
		assertThat(latest).isEqualTo(Payloads.RECORD_V5);
	}

	@Test
	void v4() {
		byte[] bytes = serializer.serialize(TOPIC, Payloads.RECORD_V4);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		GenericRecord latest = deserializer.deserialize(TOPIC, bytes);
		assertThat(latest).isEqualTo(Payloads.RECORD_V5);
	}

	@Test
	void v5() {
		byte[] bytes = serializer.serialize(TOPIC, Payloads.RECORD_V5);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		GenericRecord latest = deserializer.deserialize(TOPIC, bytes);
		assertThat(latest).isEqualTo(Payloads.RECORD_V5);
	}

}
