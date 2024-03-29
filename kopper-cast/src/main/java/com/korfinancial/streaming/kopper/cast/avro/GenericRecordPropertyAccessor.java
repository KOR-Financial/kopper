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
import org.apache.avro.util.Utf8;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

/**
 * @author Daan Gerits
 */
public class GenericRecordPropertyAccessor implements PropertyAccessor {

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class[] { GenericRecord.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		if (target == null) {
			return false;
		}

		return target instanceof GenericRecord;
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		GenericRecord record = (GenericRecord) target;

		if (record.hasField(name)) {
			return new TypedValue(convertValue(record.get(name)));
		}
		else {
			return new TypedValue(null);
		}
	}

	@Override
	public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
		return false;
	}

	@Override
	public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {
		throw new AccessException("Not allowed");
	}

	protected Object convertValue(Object input) {
		if (input == null) {
			return null;
		}

		if (input instanceof Utf8 utf8) {
			return utf8.toString();
		}

		return input;
	}

}
