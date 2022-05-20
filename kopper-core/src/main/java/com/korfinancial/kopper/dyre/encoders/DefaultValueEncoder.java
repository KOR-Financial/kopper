/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre.encoders;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.utils.Bytes;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.DyreUtils;
import com.korfinancial.kopper.dyre.ValueMappingException;

public class DefaultValueEncoder implements ValueEncoder {

	public Object encode(Schema schema, Object actualValue, Annotation[] annotations) throws ValueMappingException {
		// -- return null if the actual field value is null
		if (actualValue == null) {
			if (!schema.isNullable()) {
				throw new ValueMappingException("not allowed to set the value for a non-nullable field to null");
			}

			return null;
		}

		switch (schema.getType()) {
		case RECORD -> {
			DynamicRecord dynamicRecord = DyreUtils.expectType(DynamicRecord.class, actualValue);
			return dynamicRecord.record();
		}
		case ARRAY -> {
			List list = DyreUtils.expectType(List.class, actualValue);
			return encodeList(schema, list, annotations);
		}
		case MAP -> {
			Map m = DyreUtils.expectType(Map.class, actualValue);
			return encodeMap(schema.getValueType(), m, annotations);
		}
		case UNION -> {
			// -- unions with multiple non-null types require a custom encoder
			if (schema.getTypes().size() == 1 || (schema.getTypes().size() == 2) && (schema.isNullable())) {
				Schema actualType = null;
				for (Schema subSchema : schema.getTypes()) {
					if (subSchema.getType().equals(Schema.Type.NULL)) {
						continue;
					}

					actualType = subSchema;
				}

				return encode(actualType, actualValue, annotations);
			}

			throw new ValueMappingException("unions with multiple non-null types require a custom encoder");
		}
		default -> {
			return encodeValue(schema, actualValue);
		}
		}
	}

	GenericData.Array encodeList(Schema schema, List list, Annotation[] annotations) throws ValueMappingException {
		GenericData.Array result = new GenericData.Array(list.size(), schema);

		for (Object obj : list) {
			result.add(encode(schema.getElementType(), obj, annotations));
		}

		return result;
	}

	Map<Object, Object> encodeMap(Schema valueSchema, Map m, Annotation[] annotations) throws ValueMappingException {
		Map<Object, Object> result = new HashMap<>();

		for (Object o : m.entrySet()) {
			Map.Entry entry = (Map.Entry) o;

			result.put(encodeValue(SchemaBuilder.builder().stringType(), entry.getKey()),
					encode(valueSchema, entry.getValue(), annotations));
		}

		return result;
	}

	Object encodeValue(Schema schema, Object actualValue) throws ValueMappingException {
		if (actualValue == null) {
			return null;
		}

		switch (schema.getType()) {
		case ENUM -> {
			Enum e = DyreUtils.expectType(Enum.class, actualValue);
			return new GenericData.EnumSymbol(schema, e.name());
		}
		case FIXED -> {
			DyreUtils.expectType(String.class, actualValue);
			throw new UnsupportedOperationException("Fixed values are not supported");
		}
		case STRING -> {
			return new Utf8(DyreUtils.expectType(String.class, actualValue));
		}
		case BYTES -> {
			return new Bytes(DyreUtils.expectType(byte[].class, actualValue));
		}
		case INT -> {
			return DyreUtils.expectType(Integer.class, actualValue);
		}
		case LONG -> {
			return DyreUtils.expectType(Long.class, actualValue);
		}
		case FLOAT -> {
			return DyreUtils.expectType(Float.class, actualValue);
		}
		case DOUBLE -> {
			return DyreUtils.expectType(Double.class, actualValue);
		}
		case BOOLEAN -> {
			return DyreUtils.expectType(Boolean.class, actualValue);
		}
		case NULL -> {
			return null;
		}
		default -> throw new ValueMappingException(
				actualValue.getClass().getName() + " is not a simple value, but a " + schema.getType().getName());
		}
	}

}
