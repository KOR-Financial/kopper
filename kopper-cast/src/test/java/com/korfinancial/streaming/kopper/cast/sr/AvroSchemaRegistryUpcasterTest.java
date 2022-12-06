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

package com.korfinancial.streaming.kopper.cast.sr;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.DeclarativeUpcasterChain;
import com.korfinancial.streaming.kopper.cast.DeclarativeUpcasterContext;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;
import com.korfinancial.streaming.kopper.cast.avro.AvroHelpers;
import com.korfinancial.streaming.kopper.cast.avro.DeclarativeAvroUpcaster;
import com.korfinancial.streaming.kopper.cast.avro.Payloads;
import com.korfinancial.streaming.kopper.cast.registry.InMemoryUpcasterRegistry;
import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V1;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V2;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V3;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V4;
import static com.korfinancial.streaming.kopper.cast.avro.Schemas.SCHEMA_V5;
import static org.assertj.core.api.Assertions.assertThat;

class AvroSchemaRegistryUpcasterTest {

	private static final String SUBJECT = "my.topic";

	private static SchemaRegistryClient schemaRegistryClient;

	private UpcasterRegistry<DeclarativeUpcasterContext, DeclarativeUpcasterChain<GenericRecord>> upcasterRegistry;

	@BeforeAll
	static void beforeAll() throws RestClientException, IOException {
		schemaRegistryClient = new MockSchemaRegistryClient();
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V1), 1, 101);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V2), 2, 102);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V3), 3, 103);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V4), 4, 104);
		schemaRegistryClient.register(SUBJECT, new AvroSchema(SCHEMA_V5), 5, 105);
	}

	@BeforeEach
	void beforeEach() throws Exception {
		upcasterRegistry = new InMemoryUpcasterRegistry<>();

		// @formatter-off
		upcasterRegistry.registerChain(DeclarativeUpcasterChain.<GenericRecord>builder(SUBJECT)
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
	}

	@Test
	void performUpcast() throws UpcasterException {
		GenericRecord v1 = com.korfinancial.streaming.kopper.cast.avro.Payloads.RECORD_V1;

		DeclarativeUpcasterChain<GenericRecord> chain = upcasterRegistry.getUpcasters(SUBJECT);
		VersionedItem<GenericRecord> result = chain.doUpcast(new VersionedItem<>(v1, 1));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5_WITHOUT_AGE);
	}

}
