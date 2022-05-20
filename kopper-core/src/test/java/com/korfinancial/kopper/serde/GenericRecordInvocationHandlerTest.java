/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.korfinancial.kopper.TestUtils;
import com.korfinancial.kopper.dyre.DynamicRecords;

import static org.assertj.core.api.Assertions.assertThat;

class GenericRecordInvocationHandlerTest {

	@BeforeAll
	static void beforeAll() throws Exception {
		SchemaRegistryClient sr = MockSchemaRegistry.getClientForScope("default");
		TestUtils.registerSchema(sr, TestModel.class, TestModel.SCHEMA);

		new DynamicRecords(sr);
	}

	@Test
	void testGet() {
		String requiredValue = UUID.randomUUID().toString();
		TestEnum requiredEnum = TestEnum.Option3;

		List<String> originalList = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Map<String, String> originalMap = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
				UUID.randomUUID().toString(), UUID.randomUUID().toString());

		TestModel model = TestModel.create(requiredValue, requiredEnum, originalList, originalMap);

		assertThat(model.getRequiredValue()).isEqualTo(requiredValue);
		assertThat(model.getRequiredEnum()).isEqualTo(requiredEnum);
		assertThat(model.getStringList()).containsOnlyOnceElementsOf(originalList);
		assertThat(model.getStringMap()).containsExactlyInAnyOrderEntriesOf(originalMap);
	}

	@Test
	void testSet() {
		TestModel model = TestModel.create();

		String v = UUID.randomUUID().toString();
		List<String> l = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		Map<String, String> m = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
				UUID.randomUUID().toString(), UUID.randomUUID().toString());

		assertThat(model.getRequiredValue()).isEqualTo("");
		model.setRequiredValue(v);
		assertThat(model.getRequiredValue()).isEqualTo(v);

		assertThat(model.getOptionalValue()).isNull();
		model.setOptionalValue(v);
		assertThat(model.getOptionalValue()).isEqualTo(v);

		assertThat(model.getStringList()).isEmpty();
		model.setStringList(l);
		assertThat(model.getStringList()).containsOnlyOnceElementsOf(l);

		assertThat(model.getStringMap()).isEmpty();
		model.setStringMap(m);
		assertThat(model.getStringMap()).containsExactlyInAnyOrderEntriesOf(m);
	}

	@Test
	void testAddToSimpleList() {
		String valueToAdd = UUID.randomUUID().toString();

		TestModel model = TestModel.create();
		assertThat(model.getStringList()).isEmpty();
		model.addToStringList(valueToAdd);
		assertThat(model.getStringList()).containsOnly(valueToAdd);
	}

	@Test
	void testRemoveFromSimpleList() {
		String valueToAdd = UUID.randomUUID().toString();

		TestModel model = TestModel.create();
		model.addToStringList(valueToAdd);

		model.removeFromStringList(valueToAdd);
		assertThat(model.getStringList()).isEmpty();
	}

	@Test
	void testPutIntoSimpleMap() {
		String keyToPut = UUID.randomUUID().toString();
		String valueToPut = UUID.randomUUID().toString();

		TestModel model = TestModel.create();
		assertThat(model.getStringMap()).isEmpty();
		model.putIntoStringMap(keyToPut, valueToPut);
		assertThat(model.getStringMap()).containsOnlyKeys(keyToPut);
		assertThat(model.getStringMap().get(keyToPut)).isEqualTo(valueToPut);
	}

	@Test
	void testRemoveFromSimpleMap() {
		String keyToPut = UUID.randomUUID().toString();
		String valueToPut = UUID.randomUUID().toString();

		TestModel model = TestModel.create();
		model.putIntoStringMap(keyToPut, valueToPut);
		assertThat(model.getStringMap()).containsOnlyKeys(keyToPut);

		model.removeFromStringMap(keyToPut);
		assertThat(model.getStringMap()).isEmpty();
	}

}
