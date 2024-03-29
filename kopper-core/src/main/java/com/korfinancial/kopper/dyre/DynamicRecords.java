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

package com.korfinancial.kopper.dyre;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.korfinancial.kopper.dyre.annotations.KopperRecord;
import com.korfinancial.kopper.dyre.encoders.ValueEncoder;

import static org.apache.commons.lang3.StringUtils.capitalize;

/**
 * @author Daan Gerits
 * @author Tim Ysewyn
 */
public class DynamicRecords {

	private static DynamicRecords instance;

	public static DynamicRecords getInstance() {
		if (instance == null) {
			throw new IllegalStateException("the dynamic records instance has not been created yet");
		}

		return instance;
	}

	private final SchemaRegistryClient schemaRegistryClient;

	public DynamicRecords(SchemaRegistryClient schemaRegistryClient) {
		this.schemaRegistryClient = schemaRegistryClient;
		instance = this;
	}

	public <T extends DynamicRecord> T newRecord(Class<T> cls, Map<String, Object> initialValues) {
		KopperRecord subAnno = cls.getDeclaredAnnotation(KopperRecord.class);
		if (subAnno != null) {
			String subject = subAnno.value();
			if (subject.equals("")) {
				subject = cls.getName();
			}

			try {
				return newRecordFromSubject(cls, subject, initialValues);
			}
			catch (Exception ex) {
				throw new IllegalArgumentException(ex);
			}
		}

		throw new IllegalArgumentException(
				"Can't decide where to get the schema from: no KopperRecord annotation has been found.");
	}

	public <T extends DynamicRecord> T newRecordFromSchema(Class<T> cls, Schema schema,
			Map<String, Object> initialValues) {
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);

		initialValues.forEach((javaFieldName, o) -> {
			String avroFieldName = DyreUtils.getFieldName(javaFieldName);
			try {
				Schema.Field field = schema.getField(avroFieldName);
				if (field == null) {
					throw new RuntimeException("field " + avroFieldName + " not found on " + cls.getName());
				}

				Schema fieldSchema = schema.getField(avroFieldName).schema();
				Method fieldSetter = setterForClass(cls, javaFieldName);

				// TODO: We are using the default value encoder here, but we might want to
				// make this configurable
				builder.set(avroFieldName, ValueEncoder.DEFAULT_ENCODER.encode(fieldSchema, o,
						(fieldSetter != null) ? fieldSetter.getAnnotations() : new Annotation[] {}));
			}
			catch (ValueMappingException | NoSuchMethodException ex) {
				throw new RuntimeException(ex);
			}
		});

		GenericRecord gr = builder.build();

		return (T) Proxy.newProxyInstance(cls.getClassLoader(), new Class<?>[] { cls },
				new GenericRecordInvocationHandler(gr));
	}

	public <T extends DynamicRecord> T newRecordFromSubject(Class<T> cls, String subject,
			Map<String, Object> initialValues) throws IOException {
		if (this.schemaRegistryClient == null) {
			throw new RuntimeException("no schema registry configured");
		}

		try {
			SchemaMetadata schemaMetadata = this.schemaRegistryClient.getLatestSchemaMetadata(subject);

			ParsedSchema schema = this.schemaRegistryClient.getSchemaById(schemaMetadata.getId());

			return newRecordFromSchema(cls, (Schema) schema.rawSchema(), initialValues);
		}
		catch (RestClientException rce) {
			throw new IllegalStateException("unable to retrieve schema for subject " + subject, rce);
		}
	}

	private Method setterForClass(Class<?> cls, String fieldName) throws NoSuchMethodException {
		String methodName = String.format("set%s", capitalize(fieldName));
		List<Method> methods = Arrays.stream(cls.getMethods()).filter((m) -> m.getName().equals(methodName)).toList();

		if (methods.size() > 1) {
			throw new UnsupportedOperationException("more than one setter has been found for field " + fieldName);
		}
		else if (methods.isEmpty()) {
			return null;
		}

		return methods.get(0);
	}

}
