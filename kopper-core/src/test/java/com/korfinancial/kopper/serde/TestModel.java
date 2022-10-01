/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.DynamicRecords;
import com.korfinancial.kopper.dyre.annotations.KopperField;
import com.korfinancial.kopper.dyre.annotations.KopperRecord;

@KopperRecord
public interface TestModel extends DynamicRecord {

	// @formatter: off
	Schema SCHEMA = SchemaBuilder.builder().record("TestModel").fields().requiredString("required_value")
			.name("optional_value").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault()
			.name("required_enum").type(SchemaBuilder.enumeration("TestEnum").symbols("Option1", "Option2", "Option3"))
			.noDefault().name("optional_enum")
			.type(SchemaBuilder.unionOf().nullType().and().enumeration("TestEnum")
					.symbols("Option1", "Option2", "Option3").endUnion())
			.noDefault().name("string_list").type(SchemaBuilder.array().items(SchemaBuilder.builder().stringType()))
			.noDefault().name("string_map").type(SchemaBuilder.map().values(SchemaBuilder.builder().stringType()))
			.noDefault().endRecord();

	// @formatter: on

	static TestModel create() {
		return create("", TestEnum.Option1, new ArrayList<>(), new HashMap<>());
	}

	static TestModel create(String requiredValue, TestEnum requiredEnum, List<String> stringList,
			Map<String, String> stringMap) {
		// @formatter: off
		return DynamicRecords.getInstance().newRecord(TestModel.class, new HashMap<>() {
			{
				put("required_value", requiredValue);
				put("optional_value", null);
				put("required_enum", requiredEnum);
				put("optional_enum", null);
				put("string_list", stringList);
				put("string_map", stringMap);
			}
		});
		// @formatter: on
	}

	// -- Required Value

	String getRequiredValue();

	void setRequiredValue(String value);

	// -- Optional Value

	@KopperField(required = false)
	String getOptionalValue();

	void setOptionalValue(String value);

	// -- Required Enum

	TestEnum getRequiredEnum();

	void setRequiredEnum(TestEnum value);

	// -- Optional Enum

	@KopperField(required = false)
	TestEnum getOptionalEnum();

	void setOptionalEnum(TestEnum value);

	// -- List

	List<String> getStringList();

	void setStringList(List<String> value);

	void addToStringList(String item);

	void removeFromStringList(String item);

	// -- Map

	Map<String, String> getStringMap();

	void setStringMap(Map<String, String> value);

	void putIntoStringMap(String key, String value);

	void removeFromStringMap(String key);

}
