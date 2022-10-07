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

import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.korfinancial.kopper.TestUtils;
import com.korfinancial.kopper.dyre.DynamicRecords;
import com.korfinancial.kopper.serde.map.models.MutablePerson;
import com.korfinancial.kopper.serde.map.models.State;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicProxySerializerTest {

	private Serializer<MutablePerson> serializer;

	private Deserializer<MutablePerson> deserializer;

	@BeforeEach
	void setup() throws Exception {
		SchemaRegistryClient sr = MockSchemaRegistry.getClientForScope("default");
		TestUtils.registerSchema(sr, MutablePerson.class, "avro/MutablePerson.avsc");

		// -- create and initialize the DynamicRecords
		new DynamicRecords(sr);

		serializer = KopperSerdes.dynamicSerializer(sr);
		serializer.configure(TestUtils.serdeTestConfig(), false);

		deserializer = KopperSerdes.dynamicDeserializer(MutablePerson.class, sr);
		deserializer.configure(TestUtils.serdeTestConfig(), false);
	}

	@Test
	void serialize() {
		// -- serialize the data
		MutablePerson person = MutablePerson.create("Daan", State.Open,
				List.of(MutablePerson.create("Lord Vader", State.Closed, List.of())));

		byte[] bytes = serializer.serialize("mytopic", person);
		assertThat(bytes).isNotNull();

		// -- deserialize the previous data again
		MutablePerson otherPerson = deserializer.deserialize("mytopic", bytes);
		assertThat(otherPerson).isNotNull();
		assertThat(otherPerson.getName()).isEqualTo("Daan");
		assertThat(otherPerson.getState()).isEqualTo(State.Open);
		assertThat(otherPerson.getSiblings()).hasSize(1);
		assertThat(otherPerson.getSiblings().get(0).getName()).isEqualTo("Lord Vader");
		assertThat(otherPerson.getSiblings().get(0).getState()).isEqualTo(State.Closed);
		assertThat(otherPerson.getSiblings().get(0).getSiblings()).isEmpty();
	}

}
