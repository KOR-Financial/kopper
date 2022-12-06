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

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.korfinancial.kopper.dyre.DyreUtils;
import com.korfinancial.kopper.dyre.ValueMappingException;
import com.korfinancial.kopper.dyre.decoders.ValueDecoder;
import com.korfinancial.kopper.serde.map.models.EnumModel;
import com.korfinancial.kopper.serde.map.models.ListModel;
import com.korfinancial.kopper.serde.map.models.MapModel;
import com.korfinancial.kopper.serde.map.models.Person;
import com.korfinancial.kopper.serde.map.models.State;

class ValueDecoderTest {

	private final Random random = new Random(System.currentTimeMillis());

	private final Schema personSchema = SchemaBuilder.record("Person").fields().requiredString("name")
			.requiredInt("age").optionalString("nickname").optionalBoolean("married").endRecord();

	@Test
	void mapRecordWithPrimitiveTypes() throws ValueMappingException {
		GenericRecord record = new GenericRecordBuilder(personSchema).set("name", "my_name").set("age", 13)
				.set("married", true).set("nickname", "my_nickname").build();

		Object result = ValueDecoder.DEFAULT_DECODER.decode(Person.class, record, new Annotation[] {});
		Assertions.assertThat(result).isInstanceOf(Person.class);

		Person model = (Person) result;
		Assertions.assertThat(model.getName()).isEqualTo("my_name");
		Assertions.assertThat(model.getAge()).isEqualTo(13);
		Assertions.assertThat(model.isMarried()).isEqualTo(true);

		GenericRecord record2 = new GenericRecordBuilder(personSchema).set("name", "my_name").set("age", 13).build();

		Object result2 = ValueDecoder.DEFAULT_DECODER.decode(Person.class, record2, new Annotation[] {});
		Assertions.assertThat(result).isInstanceOf(Person.class);

		Person model2 = (Person) result2;
		Assertions.assertThat(model2.getName()).isEqualTo("my_name");
		Assertions.assertThat(model2.getAge()).isEqualTo(13);
		Assertions.assertThat(model2.isMarried()).isNull();
	}

	@Test
	void mapRecordWithEnum() throws ValueMappingException {
		Schema enumSchema = SchemaBuilder.enumeration("State").symbols("Open", "Closed");
		Schema schema = SchemaBuilder.record("Store").fields().name("state").type(enumSchema).noDefault().endRecord();

		GenericRecord record = new GenericRecordBuilder(schema)
				.set("state", new GenericData.EnumSymbol(enumSchema, "Open")).build();

		Object result = ValueDecoder.DEFAULT_DECODER.decode(EnumModel.class, record, new Annotation[] {});
		Assertions.assertThat(result).isInstanceOf(EnumModel.class);

		EnumModel model = (EnumModel) result;
		Assertions.assertThat(model.getState()).isEqualTo(State.Open);
	}

	@Test
	void mapRecordWithList() throws ValueMappingException {
		Schema listSchema = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
		Schema personListSchema = SchemaBuilder.array().items(personSchema);
		Schema schema = SchemaBuilder.record("ListModel").fields().name("string_list").type(listSchema).noDefault()
				.name("person_list").type(personListSchema).noDefault().endRecord();

		List<GenericRecord> people = List.of(createRandomPerson(false, false), createRandomPerson(true, false),
				createRandomPerson(false, true), createRandomPerson(true, true));

		GenericRecord record = new GenericRecordBuilder(schema)
				.set("string_list", new GenericData.Array<>(listSchema, List.of("v1", "v2", "v3")))
				.set("person_list", new GenericData.Array<>(personListSchema, people)).build();

		Object result = ValueDecoder.DEFAULT_DECODER.decode(ListModel.class, record, new Annotation[] {});
		Assertions.assertThat(result).isInstanceOf(ListModel.class);

		ListModel model = (ListModel) result;
		Assertions.assertThat(model.getStringList()).hasSize(3);
		Assertions.assertThat(model.getStringList().get(0)).isEqualTo("v1");

		Assertions.assertThat(model.getPersonList()).hasSize(4);
		for (int i = 0; i < model.getPersonList().size(); i++) {
			GenericRecord original = people.get(i);
			Person actual = model.getPersonList().get(i);

			Assertions.assertThat(actual.getName()).isEqualTo(original.get("name"));
			Assertions.assertThat(actual.getAge()).isEqualTo(original.get("age"));
			Assertions.assertThat(actual.isMarried()).isEqualTo(original.get("married"));
		}
	}

	@Test
	void mapRecordWithMap() throws ValueMappingException {
		Schema mapSchema = SchemaBuilder.map().values(SchemaBuilder.builder().stringType());
		Schema personMapSchema = SchemaBuilder.map().values(personSchema);
		Schema schema = SchemaBuilder.record("MapModel").fields().name("string_map").type(mapSchema).noDefault()
				.name("person_map").type(personMapSchema).noDefault().endRecord();

		Map<String, GenericRecord> people = Map.of("k1", createRandomPerson(false, false), "k2",
				createRandomPerson(true, false), "k3", createRandomPerson(false, true), "k4",
				createRandomPerson(true, true));

		GenericRecord record = new GenericRecordBuilder(schema)
				.set("string_map", Map.of("k1", "v1", "k2", "v2", "k3", "v3")).set("person_map", people).build();

		Object result = ValueDecoder.DEFAULT_DECODER.decode(MapModel.class, record, new Annotation[] {});
		Assertions.assertThat(result).isInstanceOf(MapModel.class);

		MapModel model = (MapModel) result;
		Assertions.assertThat(model.getStringMap()).hasSize(3);
		Assertions.assertThat(model.getStringMap().get("k1")).isEqualTo("v1");

		Assertions.assertThat(model.getPersonMap()).hasSize(4);
		for (String key : model.getPersonMap().keySet()) {
			GenericRecord original = people.get(key);
			Person actual = model.getPersonMap().get(key);

			Assertions.assertThat(actual.getName()).isEqualTo(original.get("name"));
			Assertions.assertThat(actual.getAge()).isEqualTo(original.get("age"));
			Assertions.assertThat(actual.isMarried()).isEqualTo(original.get("married"));
		}
	}

	@Test
	void encodeStringToString() throws ValueMappingException {
		Object input = "my test string";

		Object result = ValueDecoder.DEFAULT_DECODER.decode(String.class, input, new Annotation[] {});

		Assertions.assertThat(result).isEqualTo(input);
	}

	@Test
	void encodeAvroUtf8ToString() throws ValueMappingException {
		String input = "my test string";

		Object result = ValueDecoder.DEFAULT_DECODER.decode(String.class, new Utf8(input), new Annotation[] {});

		Assertions.assertThat(result).isEqualTo(input);
	}

	@Test
	void encodeAvroUtf8ListToStringList() throws ValueMappingException {
		List<String> input = List.of("v1", "v2", "v3");

		Schema arrSchema = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
		GenericData.Array<Utf8> arrUtf8 = new GenericData.Array<Utf8>(1, arrSchema);
		for (String s : input) {
			arrUtf8.add(new Utf8(s));
		}

		Object result = ValueDecoder.DEFAULT_DECODER.decode(new StringListType(), arrUtf8,
				new Annotation[] { DyreUtils.kopperField(String.class, false) });

		Assertions.assertThat(result).isEqualTo(input);
	}

	GenericRecord createRandomPerson(boolean withNickname, boolean withMarried) {
		GenericRecordBuilder builder = new GenericRecordBuilder(personSchema).set("name", UUID.randomUUID().toString())
				.set("age", random.nextInt(100));

		if (withMarried) {
			builder.set("married", random.nextBoolean());
		}

		if (withNickname) {
			builder.set("nickname", UUID.randomUUID().toString());
		}

		return builder.build();
	}

	// This could be done with
	// ParameterizedTypeImpl.make(List.class, new Type[] { String.class }, null);
	// But it's not being exported via module-info.java, so we can't use it without
	// hacking things together
	private static class StringListType implements ParameterizedType {

		@Override
		public Type[] getActualTypeArguments() {
			return new Type[] { String.class };
		}

		@Override
		public Type getRawType() {
			return List.class;
		}

		@Override
		public Type getOwnerType() {
			return null;
		}

	}

}
