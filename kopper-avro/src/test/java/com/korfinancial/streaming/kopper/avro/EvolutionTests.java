/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.avro;

import java.io.IOException;
import java.util.List;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.Before;
import org.junit.Test;

import static com.korfinancial.streaming.kopper.avro.Utils.registerSchema;
import static org.assertj.core.api.Assertions.assertThat;

public class EvolutionTests {

	private SchemaRegistryClient schemaRegistryClient;

	@Before
	public void beforeEach() {
		schemaRegistryClient = MockSchemaRegistry.getClientForScope("");
	}

	@Test
	public void shouldNotAllowRenamingAField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1-invalid");

		assertThat(v1.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0))).isNotEmpty();
	}

	@Test
	public void shouldAllowAddRequiredField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");

		assertThat(v1.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0))).isEmpty();
	}

	@Test
	public void shouldAllowAddOptionalField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");

		assertThat(v2.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1))).isEmpty();
	}

	@Test
	public void shouldAllowScrambleFieldOrder() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");

		assertThat(v3.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2))).isEmpty();
	}

	@Test
	public void shouldAllowRemoveOptionalField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");

		assertThat(v4.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3))).isEmpty();
	}

	@Test
	public void shouldNotAllowRemoveRequiredField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4-invalid");

		assertThat(v4.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3))).isNotEmpty();
	}

	@Test
	public void shouldAllowAddingSymbolToEnumWithDefault() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");
		AvroSchema v5 = registerSchema(schemaRegistryClient, "person-v5");
		AvroSchema v6 = registerSchema(schemaRegistryClient, "person-v6");

		assertThat(v6.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3, v4, v5))).isEmpty();
	}

	@Test
	public void shouldNotAllowAddingSymbolToEnumWithoutDefault() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");
		AvroSchema v5 = registerSchema(schemaRegistryClient, "person-v5");
		AvroSchema v6 = registerSchema(schemaRegistryClient, "person-v6");
		AvroSchema v7 = registerSchema(schemaRegistryClient, "person-v7-invalid");

		assertThat(v7.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3, v4, v5, v6)))
				.isNotEmpty();
	}

	@Test
	public void shouldAllowScramblingSymbols() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");
		AvroSchema v5 = registerSchema(schemaRegistryClient, "person-v5");
		AvroSchema v6 = registerSchema(schemaRegistryClient, "person-v6");
		AvroSchema v7 = registerSchema(schemaRegistryClient, "person-v7");

		assertThat(v7.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3, v4, v5, v6)))
				.isEmpty();
	}

	@Test
	public void shouldAllowAddingSymbolAnywhereToEnumWithDefault() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");
		AvroSchema v5 = registerSchema(schemaRegistryClient, "person-v5");
		AvroSchema v6 = registerSchema(schemaRegistryClient, "person-v6");
		AvroSchema v7 = registerSchema(schemaRegistryClient, "person-v7");
		AvroSchema v8 = registerSchema(schemaRegistryClient, "person-v8");

		assertThat(v8.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3, v4, v5, v6, v7)))
				.isEmpty();
	}

	@Test
	public void shouldAllowAddingAnAliasForAField() throws RestClientException, IOException {
		AvroSchema v0 = registerSchema(schemaRegistryClient, "person-v0");
		AvroSchema v1 = registerSchema(schemaRegistryClient, "person-v1");
		AvroSchema v2 = registerSchema(schemaRegistryClient, "person-v2");
		AvroSchema v3 = registerSchema(schemaRegistryClient, "person-v3");
		AvroSchema v4 = registerSchema(schemaRegistryClient, "person-v4");
		AvroSchema v5 = registerSchema(schemaRegistryClient, "person-v5");
		AvroSchema v6 = registerSchema(schemaRegistryClient, "person-v6");
		AvroSchema v7 = registerSchema(schemaRegistryClient, "person-v7");
		AvroSchema v8 = registerSchema(schemaRegistryClient, "person-v8");
		AvroSchema v9 = registerSchema(schemaRegistryClient, "person-v9");

		assertThat(v9.isCompatible(CompatibilityLevel.FORWARD_TRANSITIVE, List.of(v0, v1, v2, v3, v4, v5, v6, v7, v8)))
				.isEmpty();
	}

}
