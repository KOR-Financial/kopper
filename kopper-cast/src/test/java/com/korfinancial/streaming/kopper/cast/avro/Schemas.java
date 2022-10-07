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
