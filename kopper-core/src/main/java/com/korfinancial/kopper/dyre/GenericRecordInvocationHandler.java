/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.korfinancial.kopper.dyre.decoders.ValueDecoder;
import com.korfinancial.kopper.dyre.encoders.ValueEncoder;

import static org.apache.commons.lang3.StringUtils.capitalize;

public class GenericRecordInvocationHandler implements InvocationHandler {

	private final GenericRecord record;

	private final ValueDecoder valueDecoder;

	private final ValueEncoder valueEncoder;

	public GenericRecordInvocationHandler(GenericRecord record) {
		this(record, ValueDecoder.DEFAULT_DECODER, ValueEncoder.DEFAULT_ENCODER);
	}

	public GenericRecordInvocationHandler(GenericRecord record, ValueDecoder valueDecoder, ValueEncoder valueEncoder) {
		this.record = record;
		this.valueDecoder = valueDecoder;
		this.valueEncoder = valueEncoder;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (method.getName().startsWith("get")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "get");

			return getFieldValue(field, method);
		}
		else if (method.getName().startsWith("is")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "is");

			return getFieldValue(field, method);
		}
		else if (method.getName().startsWith("set")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "set");

			setFieldValue(field, method, args[0]);

			return null;
		}
		else if (method.getName().startsWith("addTo")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "addTo");
			Method getter = getterForMethod(proxy.getClass().getInterfaces()[0], method, "addTo");
			Method setter = setterForGetter(proxy.getClass().getInterfaces()[0], getter);

			addValueToField(field, getter, setter, args[0]);

			return null;
		}
		else if (method.getName().startsWith("putInto")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "putInto");
			Method getter = getterForMethod(proxy.getClass().getInterfaces()[0], method, "putInto");
			Method setter = setterForGetter(proxy.getClass().getInterfaces()[0], getter);

			putValueIntoField(field, getter, setter, (String) args[0], args[1]);

			return null;
		}
		else if (method.getName().startsWith("removeFrom")) {
			// -- get the field name the record is pointing to
			String field = DyreUtils.getFieldName(method, "removeFrom");
			Method getter = getterForMethod(proxy.getClass().getInterfaces()[0], method, "removeFrom");
			Method setter = setterForGetter(proxy.getClass().getInterfaces()[0], getter);

			removeFromField(field, getter, setter, args[0]);

			return null;
		}
		else if (method.getName().equals("record")) {
			return record;
		}
		else if (method.getName().equals("retrieveDirect")) {
			Class<? extends DynamicRecord> cls = (Class<? extends DynamicRecord>) args[0];
			String field = (String) args[1];

			Method getter = cls.getMethod("get" + capitalize(field));
			return getter.invoke(proxy);
		}
		else if (method.getName().equals("manipulateDirect")) {
			Class<? extends DynamicRecord> cls = (Class<? extends DynamicRecord>) args[0];
			String field = (String) args[1];

			Method setter = null;
			for (Method plausibleSetter : cls.getMethods()) {
				if (plausibleSetter.getName().equals("set" + capitalize(field))) {
					setter = plausibleSetter;
					break;
				}
			}

			if (setter == null) {
				throw new ValueMappingException("no setter has been found for field " + field);
			}

			return setter.invoke(proxy, args[2]);
		}
		else if (method.getName().equals("equals")) {
			if (args[0] == null && record != null) {
				return false;
			}

			if (!(args[0] instanceof DynamicRecord d)) {
				return false;
			}

			return d.record().equals(record);
		}
		else if (method.getName().equals("toString")) {
			return record.toString();
		}
		else {
			throw new UnsupportedOperationException("unsupported method '" + method.getName() + "'");
		}
	}

	void addValueToField(String fieldName, Method getter, Method setter, Object valueToAdd)
			throws ValueMappingException {
		Object fieldValue = getFieldValue(fieldName, getter);

		if ((fieldValue != null) && (!(fieldValue instanceof List))) {
			throw new IllegalArgumentException("value of field " + fieldName + " is not a list");
		}

		List list = new ArrayList();
		if (fieldValue != null) {
			list.addAll((List) fieldValue);
		}

		list.add(valueToAdd);
		setFieldValue(fieldName, setter, list);
	}

	void putValueIntoField(String fieldName, Method getter, Method setter, String key, Object value)
			throws ValueMappingException {
		Object fieldValue = getFieldValue(fieldName, getter);

		if ((fieldValue != null) && (!(fieldValue instanceof Map))) {
			throw new IllegalArgumentException("value of field " + fieldName + " is not a map");
		}

		Map result = new HashMap();
		if (fieldValue != null) {
			result.putAll((Map) fieldValue);
		}

		result.put(key, value);

		setFieldValue(fieldName, setter, result);
	}

	void removeFromField(String fieldName, Method getter, Method setter, Object itemOrKey)
			throws ValueMappingException {
		Object fieldValue = getFieldValue(fieldName, getter);
		if (fieldValue == null) {
			return;
		}

		if (fieldValue instanceof List l) {
			l = new ArrayList(l);
			l.remove(itemOrKey);
			setFieldValue(fieldName, setter, l);
		}
		else if (fieldValue instanceof Map m) {
			m = new HashMap(m);
			m.remove(itemOrKey);
			setFieldValue(fieldName, setter, m);
		}
		else {
			throw new IllegalArgumentException("value of field " + fieldName + " is not a map or list");
		}
	}

	Object getFieldValue(String fieldName, Method method) throws UnsupportedOperationException, ValueMappingException {
		if (!record.hasField(fieldName)) {
			throw new UnsupportedOperationException("no '" + fieldName + "' field available on the record");
		}

		Object actualValue = record.get(fieldName);

		return valueDecoder.decode(method.getGenericReturnType(), actualValue, method.getAnnotations());
	}

	void setFieldValue(String fieldName, Method method, Object newValue) throws ValueMappingException {
		if (!record.hasField(fieldName)) {
			throw new UnsupportedOperationException("no '" + fieldName + "' field available on the record");
		}

		Schema fieldSchema = record.getSchema().getField(fieldName).schema();

		Object toSet = valueEncoder.encode(fieldSchema, newValue, method.getParameterAnnotations()[0]);

		record.put(fieldName, toSet);
	}

	Method getterForMethod(Class<?> cls, Method calledMethod, String prefix) throws NoSuchMethodException {
		String methodName = calledMethod.getName().replaceFirst(prefix, "get");
		return cls.getDeclaredMethod(methodName);
	}

	Method setterForGetter(Class<?> cls, Method getter) throws NoSuchMethodException {
		String methodName = getter.getName().replaceFirst("get", "set");
		return cls.getDeclaredMethod(methodName, getter.getReturnType());
	}

}
