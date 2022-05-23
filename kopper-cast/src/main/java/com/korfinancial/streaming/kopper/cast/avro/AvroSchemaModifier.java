/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class AvroSchemaModifier {

	public Schema addField(Schema schema, Schema.Field field) {
		if (schema.getType() != Schema.Type.RECORD) {
			throw new IllegalArgumentException("the provided schema must be a record schema");
		}

		schema.getFields().add(field);

		return schema;
	}

	public Schema renameField(Schema schema, String oldFieldName, String newFieldName) {
		return schema;
	}

	public Schema changeFieldType(Schema schema, String fieldName, Schema fieldType) {
		return schema;
	}

	public Schema removeField(Schema schema, String field) {
		return schema;
	}
}
