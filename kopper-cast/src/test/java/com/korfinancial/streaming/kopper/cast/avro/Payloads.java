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

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public abstract class Payloads {

	// @formatter-off

	public static GenericRecord RECORD_V1 = new GenericRecordBuilder(Schemas.SCHEMA_V1).set("name", "Daan Gerits")
			.set("username", "daang").build();

	public static GenericRecord RECORD_V2_WITH_AGE = new GenericRecordBuilder(Schemas.SCHEMA_V2)
			.set("name", "Daan Gerits").set("username", "daang").set("age", "39").build();

	public static GenericRecord RECORD_V2_WITHOUT_AGE = new GenericRecordBuilder(Schemas.SCHEMA_V2)
			.set("name", "Daan Gerits").set("username", "daang").build();

	public static GenericRecord RECORD_V3 = new GenericRecordBuilder(Schemas.SCHEMA_V3).set("name", "Daan Gerits")
			.set("username", "daang").set("age", 39).build();

	public static GenericRecord RECORD_V3_WITHOUT_AGE = new GenericRecordBuilder(Schemas.SCHEMA_V3)
			.set("name", "Daan Gerits").set("username", "daang").build();

	public static GenericRecord RECORD_V4 = new GenericRecordBuilder(Schemas.SCHEMA_V4).set("firstname", "Daan")
			.set("lastname", "Gerits").set("username", "daang").set("age", 39).build();

	public static GenericRecord RECORD_V4_WITHOUT_AGE = new GenericRecordBuilder(Schemas.SCHEMA_V4)
			.set("firstname", "Daan").set("lastname", "Gerits").set("username", "daang").build();

	public static GenericRecord RECORD_V5 = new GenericRecordBuilder(Schemas.SCHEMA_V5)
			.set("name", new GenericRecordBuilder(Schemas.SCHEMA_V5.getField("name").schema()).set("firstname", "Daan")
					.set("lastname", "Gerits").build())
			.set("username", "daang").set("age", 39).build();

	public static GenericRecord RECORD_V5_WITHOUT_AGE = new GenericRecordBuilder(Schemas.SCHEMA_V5)
			.set("name", new GenericRecordBuilder(Schemas.SCHEMA_V5.getField("name").schema()).set("firstname", "Daan")
					.set("lastname", "Gerits").build())
			.set("username", "daang").build();

	// @formatter-on

}
