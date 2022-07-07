/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre.decoders;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.GenericRecordInvocationHandler;
import com.korfinancial.kopper.dyre.ValueMappingException;

public class DefaultValueDecoder implements ValueDecoder {

	final List<Class<?>> directTypes = List.of(byte[].class, Integer.class, Long.class, Float.class, Double.class,
			Boolean.class, String.class);

	public Object decode(Type expectedType, Object actualValue, Annotation[] annotations) throws ValueMappingException {
		// -- return null if the actual field value is null
		if (actualValue == null) {
			return null;
		}

		if (expectedType instanceof Class<?>) {
			Class<?> expectedClassType = (Class<?>) expectedType;
			if (DynamicRecord.class.isAssignableFrom(expectedClassType)) {
				if (actualValue instanceof GenericRecord v) {
					return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { expectedClassType },
							new GenericRecordInvocationHandler(v));
				}

				throw new ValueMappingException(
						actualValue.getClass().getName() + " is not a " + GenericRecord.class.getName());
			}
			else {
				return decodeValue(expectedClassType, actualValue);
			}
		}
		else if (expectedType instanceof ParameterizedType) {
			ParameterizedType parameterizedExpectedType = (ParameterizedType) expectedType;
			Class<?> rawReturnType = (Class<?>) parameterizedExpectedType.getRawType();

			if (List.class.isAssignableFrom(rawReturnType)) {
				return decodeList((Class<?>) parameterizedExpectedType.getActualTypeArguments()[0], actualValue);
			}
			else if (Map.class.isAssignableFrom(rawReturnType)) {
				return decodeMap((Class<?>) parameterizedExpectedType.getActualTypeArguments()[0],
						(Class<?>) parameterizedExpectedType.getActualTypeArguments()[1], actualValue);
			}
		}

		throw new ValueMappingException("Reached the end of the line. We have no idea what's happening!");
	}

	List<Object> decodeList(Class<?> expectedValueType, Object in) throws ValueMappingException {
		if (!(in instanceof GenericData.Array<?> array)) {
			throw new ValueMappingException(
					"Expected a " + GenericData.Array.class.getName() + " but received a " + in.getClass().getName());
		}

		List<Object> result = new ArrayList<>();

		for (Object obj : array) {
			result.add(decode(expectedValueType, obj, new Annotation[] {}));
		}

		return Collections.unmodifiableList(result);
	}

	Map<Object, Object> decodeMap(Class<?> expectedKeyType, Class<?> expectedValueType, Object in)
			throws ValueMappingException {
		if (!(in instanceof Map m)) {
			throw new ValueMappingException(
					"Expected a " + GenericData.Array.class.getName() + " but received a " + in.getClass().getName());
		}

		Map<Object, Object> result = new HashMap<>();

		for (Object o : m.entrySet()) {
			Map.Entry entry = (Map.Entry) o;

			result.put(decodeValue(expectedKeyType, entry.getKey()),
					decode(expectedValueType, entry.getValue(), new Annotation[] {}));
		}

		return Collections.unmodifiableMap(result);
	}

	Object decodeValue(Class<?> expected, Object in) throws ValueMappingException {
		if (in == null) {
			return null;
		}

		// -- evaluate enums before skipping for direct types. Enums are strings in a
		// generic record and will be
		// -- passed directly otherwise
		if (expected.isEnum()) {
			String value = null;
			if (in instanceof GenericData.EnumSymbol s) {
				value = s.toString();
			}
			else if (in instanceof String s) {
				value = s;
			}
			else {
				throw new ValueMappingException("Expected a " + GenericData.EnumSymbol.class.getName()
						+ "or java.lang.String but received a " + in.getClass().getName());
			}

			return Enum.valueOf((Class<? extends Enum>) expected, value);
		}

		if (directTypes.contains(in.getClass())) {
			return in;
		}

		if (in instanceof Utf8 a) {
			return new String(a.getBytes());
		}

		throw new ValueMappingException("Unable to map " + in.getClass().getName() + " to " + expected.getName());
	}

}
