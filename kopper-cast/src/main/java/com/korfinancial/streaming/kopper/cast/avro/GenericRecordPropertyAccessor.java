/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

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
			return new TypedValue(record.get(name));
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

}
