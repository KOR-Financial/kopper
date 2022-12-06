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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterContext;

/**
 * @author Daan Gerits
 */
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
	public GenericRecord upcast(BasicUpcasterContext ctx, GenericRecord input, Integer inputVersion)
			throws UpcasterException {
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
