/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.expressions;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.avro.Schemas;

import static org.assertj.core.api.Assertions.assertThat;

class SpelEvaluatorTest {

	public static GenericRecord RECORD = new GenericRecordBuilder(Schemas.SCHEMA_V1).set("name", "Daan Gerits")
			.set("username", "daang").build();

	@Test
	void testEvalGenericRecord() {
		SpelEvaluator evaluator = new SpelEvaluator(Map.of("my_field", "'Hello ' + #input.name"));

		evaluator.setVariable("input", RECORD);

		Object value = evaluator.eval("my_field");
		assertThat(value).isEqualTo("Hello Daan Gerits");
	}

}
