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
