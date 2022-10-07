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
