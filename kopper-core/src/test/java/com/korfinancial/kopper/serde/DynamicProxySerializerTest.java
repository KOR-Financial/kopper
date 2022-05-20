/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
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
