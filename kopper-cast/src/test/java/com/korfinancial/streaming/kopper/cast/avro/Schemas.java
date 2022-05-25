/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public abstract class Schemas {

	// @formatter-off

	public static final Schema SCHEMA_V1 = SchemaBuilder.record("User").fields().requiredString("username")
			.requiredString("name").endRecord();

	// -- v2 adds a new optional string field.
	public static final Schema SCHEMA_V2 = SchemaBuilder.record("User").fields().requiredString("username")
			.requiredString("name").optionalString("age").endRecord();

	// -- v3 changes the type of the age field to an integer
	public static final Schema SCHEMA_V3 = SchemaBuilder.record("User").fields().requiredString("username")
			.requiredString("name").optionalInt("age").endRecord();

	// -- v4 replaces the name field with two other fields
	public static final Schema SCHEMA_V4 = SchemaBuilder.record("User").fields().requiredString("username")
			.requiredString("firstname").requiredString("lastname").optionalInt("age").endRecord();

	public static final Schema SCHEMA_V5 = SchemaBuilder
			.record("User").fields().requiredString("username").optionalInt("age").name("name").type(SchemaBuilder
					.record("Name").fields().requiredString("firstname").requiredString("lastname").endRecord())
			.noDefault().endRecord();

	// @formatter-on

}
