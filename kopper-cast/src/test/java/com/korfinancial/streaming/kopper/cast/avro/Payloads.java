/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
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
