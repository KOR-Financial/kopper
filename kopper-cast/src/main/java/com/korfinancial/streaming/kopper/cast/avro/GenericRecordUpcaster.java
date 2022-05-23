/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import com.korfinancial.streaming.kopper.cast.Upcaster;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public abstract class GenericRecordUpcaster implements Upcaster<GenericRecord, Integer> {
	private final Schema targetSchema;
	private final Integer targetSchemaVersion;

	public GenericRecordUpcaster(Schema targetSchema, Integer targetSchemaVersion) {
		this.targetSchema = targetSchema;
		this.targetSchemaVersion = targetSchemaVersion;
	}

	@Override
	public Integer getTargetVersion() {
		return targetSchemaVersion;
	}

	@Override
	public GenericRecord upcast(GenericRecord input) {
		GenericRecordBuilder builder = new GenericRecordBuilder(targetSchema);

		for (Schema.Field inputField : input.getSchema().getFields()) {
			Schema.Field targetField = targetSchema.getField(inputField.name());

			if (targetField != null) {
				builder.set(targetField, input.get(inputField.name()));
			}
		}

		return upcast(builder, input).build();
	}

	public abstract GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input);
}
