/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import com.korfinancial.streaming.kopper.cast.UpcasterContext;

import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterContext;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;

public abstract class AvroUpcaster implements Upcaster<GenericRecord, BasicUpcasterContext> {

	private final Schema targetSchema;

	private final Integer targetSchemaVersion;

	public AvroUpcaster(Schema targetSchema, Integer targetSchemaVersion) {
		this.targetSchema = targetSchema;
		this.targetSchemaVersion = targetSchemaVersion;
	}

	@Override
	public Integer getTargetVersion() {
		return targetSchemaVersion;
	}

	public Schema getTargetSchema() {
		return targetSchema;
	}

	@Override
	public GenericRecord upcast(BasicUpcasterContext ctx, GenericRecord input, Integer inputVersion) throws UpcasterException {
		GenericRecordBuilder builder = new GenericRecordBuilder(targetSchema);

		try {
			for (Schema.Field inputField : input.getSchema().getFields()) {
				Schema.Field targetField = targetSchema.getField(inputField.name());

				if (targetField != null) {
					builder.set(targetField, input.get(inputField.name()));
				}
			}

			return upcast(builder, input).build();
		}
		catch (Exception ex) {
			throw new UpcasterException("unable to cast " + input.getSchema().getFullName() + "@" + inputVersion
					+ " to " + targetSchema.getFullName() + "@" + targetSchemaVersion, ex);
		}
	}

	public abstract GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input)
			throws UpcasterException;

}
