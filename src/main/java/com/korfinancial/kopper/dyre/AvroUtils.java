/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.mapstruct.ap.internal.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.korfinancial.kopper.dyre.annotations.KopperField;

import static com.korfinancial.kopper.dyre.DyreUtils.getFieldName;

public abstract class AvroUtils {

	private static final Logger logger = LoggerFactory.getLogger(AvroUtils.class);

	public static Schema schemaFromClass(Class<? extends DynamicRecord> recordClass) throws ValueMappingException {
		Schema schema = Schema.createRecord(recordClass.getSimpleName(), null, null, false);

		// -- detect the methods
		Map<String, Method> getters = new HashMap<>();
		Map<String, Method> setters = new HashMap<>();
		Map<String, Method> adders = new HashMap<>();
		Map<String, Method> removers = new HashMap<>();
		Map<String, Map<Class<? extends Annotation>, Annotation>> annotations = new HashMap<>();

		for (Method method : recordClass.getDeclaredMethods()) {
			if (method.getName().startsWith("get")) {
				String fieldName = getFieldName(method, "get");
				getters.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
			else if (method.getName().startsWith("is")) {
				String fieldName = getFieldName(method, "is");
				getters.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
			else if (method.getName().startsWith("set")) {
				String fieldName = getFieldName(method, "set");
				setters.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
			else if (method.getName().startsWith("addTo")) {
				String fieldName = getFieldName(method, "addTo");
				adders.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getParameterAnnotations()[0]);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
			else if (method.getName().startsWith("putInto")) {
				String fieldName = getFieldName(method, "putInto");
				adders.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getParameterAnnotations()[0]);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
			else if (method.getName().startsWith("removeFrom")) {
				String fieldName = getFieldName(method, "removeFrom");
				removers.put(fieldName, method);
				addAnnotations(annotations, fieldName, method.getDeclaredAnnotations());
			}
		}

		// -- determine the field names
		Set<String> fieldNames = Collections.asSet(getters.keySet(), setters.keySet(), adders.keySet(),
				removers.keySet());

		// -- determine field annotations
		List<Schema.Field> fields = new ArrayList<>();
		for (String fieldName : fieldNames) {
			try {
				Class<?> fieldClass = getClassOfField(fieldName, getters.get(fieldName), setters.get(fieldName));
				Schema fieldSchema = schemaForField(fieldClass, annotations.get(fieldName));
				fields.add(new Schema.Field(fieldName, fieldSchema));
			}
			catch (ValueMappingException vme) {
				throw new ValueMappingException("failed to map field " + fieldName + ": " + vme.getMessage(), vme);
			}
		}
		schema.setFields(fields);

		return schema;
	}

	private static void addAnnotations(Map<String, Map<Class<? extends Annotation>, Annotation>> annotations,
			String fieldName, Annotation[] toAdd) {
		annotations.computeIfAbsent(fieldName, (k) -> new HashMap<>());

		for (Annotation annotation : toAdd) {
			if (annotations.get(fieldName).containsKey(annotation.annotationType())) {
				logger.debug("duplicate annotation " + annotation + " detected for field " + fieldName);
				continue;
			}

			annotations.get(fieldName).put(annotation.annotationType(), annotation);
		}
	}

	private static Schema schemaForField(Class<?> type, Map<Class<? extends Annotation>, Annotation> annotations)
			throws ValueMappingException {
		KopperField anno = (KopperField) annotations.get(KopperField.class);

		if (List.class.isAssignableFrom(type)) {
			if (anno == null) {
				throw new ValueMappingException("No KopperField annotation provided.");
			}

			return makeOptionalIfNeeded(SchemaBuilder.array().items(schemaForField(anno.itemType(), annotations)),
					anno);

		}
		else if (Map.class.isAssignableFrom(type)) {
			if (anno == null) {
				throw new ValueMappingException("No KopperField annotation provided.");
			}

			return makeOptionalIfNeeded(SchemaBuilder.map().values(schemaForField(anno.itemType(), annotations)), anno);

		}
		else if (type.isEnum()) {
			List<String> symbols = new ArrayList<>();
			for (Enum<?> ec : ((Class<? extends Enum<?>>) type).getEnumConstants()) {
				symbols.add(ec.name());
			}

			Schema enumSchema = SchemaBuilder.enumeration(type.getSimpleName())
					.symbols(symbols.toArray(new String[] {}));
			return makeOptionalIfNeeded(enumSchema, anno);

		}
		else if (DynamicRecord.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(schemaFromClass((Class<? extends DynamicRecord>) type), anno);

		}
		else if (String.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().stringType(), anno);
		}
		else if (Boolean.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().booleanType(), anno);
		}
		else if (Integer.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().intType(), anno);
		}
		else if (Long.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().longType(), anno);
		}
		else if (Float.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().floatType(), anno);
		}
		else if (Double.class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().doubleType(), anno);
		}
		else if (byte[].class.isAssignableFrom(type)) {
			return makeOptionalIfNeeded(SchemaBuilder.builder().bytesType(), anno);
		}
		else {
			throw new ValueMappingException(type.getName() + ": unsupported type");
		}
	}

	private static Class<?> getClassOfField(String fieldName, Method getter, Method setter)
			throws ValueMappingException {
		if (getter != null) {
			return getter.getReturnType();
		}

		if (setter != null && setter.getParameterTypes().length == 1) {
			return setter.getParameterTypes()[0];
		}

		throw new ValueMappingException("No getter or setter found for field " + fieldName);
	}

	private static Schema makeOptionalIfNeeded(Schema schema, KopperField anno) {
		if (anno != null && !anno.required()) {
			return SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
		}
		else {
			return schema;
		}
	}

}
