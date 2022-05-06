/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde.map;

import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.korfinancial.kopper.TestUtils;
import com.korfinancial.kopper.dyre.DynamicRecords;
import com.korfinancial.kopper.serde.map.models.MutablePerson;
import com.korfinancial.kopper.serde.map.models.State;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueEncoderTest {

	@BeforeEach
	void setup() throws Exception {
		SchemaRegistryClient sr = MockSchemaRegistry.getClientForScope("default");
		TestUtils.registerSchema(sr, MutablePerson.class, "avro/MutablePerson.avsc");

		// -- create and initialize the DynamicRecords
		new DynamicRecords(sr);
	}

	@Test
	void encodePersonTest() {
		MutablePerson mutablePerson = MutablePerson.create("Daan", State.Open, List.of());

		assertThat(mutablePerson).isNotNull();
		assertThat(mutablePerson.getName()).isEqualTo("Daan");
		assertThat(mutablePerson.getState()).isEqualTo(State.Open);
		assertThat(mutablePerson.getSiblings()).hasSize(0);
	}

	@Test
	void encodeComplexPersonTest() throws Exception {
		MutablePerson daan = MutablePerson.create("Daan", State.Open,
				List.of(MutablePerson.create("Lord Vader", State.Closed, List.of())));

		assertThat(daan).isNotNull();
		assertThat(daan.getName()).isEqualTo("Daan");
		assertThat(daan.getState()).isEqualTo(State.Open);
		assertThat(daan.getSiblings()).hasSize(1);

		MutablePerson sib = daan.getSiblings().get(0);
		assertThat(sib.getName()).isEqualTo("Lord Vader");
		assertThat(sib.getState()).isEqualTo(State.Closed);
		assertThat(sib.getSiblings()).hasSize(0);
	}

}
