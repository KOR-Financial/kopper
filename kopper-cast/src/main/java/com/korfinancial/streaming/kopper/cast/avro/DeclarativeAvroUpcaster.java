/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.expressions.Evaluator;
import com.korfinancial.streaming.kopper.cast.expressions.SpelEvaluator;

public final class DeclarativeAvroUpcaster<V extends Comparable<V>> implements Upcaster<GenericRecord, V> {

	private final Schema targetSchema;

	private final V targetSchemaVersion;

	private final Evaluator evaluator;

	public static <V extends Comparable<V>> Builder<V> builder(Schema schema, V version) {
		return new Builder<>(schema, version);
	}

	public static Builder<Integer> builder(SchemaRegistryClient schemaRegistryClient, String subject, Integer version) {
		// -- get the schema
		io.confluent.kafka.schemaregistry.client.rest.entities.Schema s = schemaRegistryClient.getByVersion(subject,
				version, true);
		if (s == null) {
			throw new IllegalArgumentException(
					"unable to find a schema for subject " + subject + " version " + version);
		}

		try {
			ParsedSchema parsedSchema = schemaRegistryClient.getSchemaById(s.getId());
			if (parsedSchema == null) {
				throw new IllegalArgumentException("unable to find a schema for subject " + subject + " version "
						+ version + " with id " + s.getId());
			}

			org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) parsedSchema.rawSchema();

			return DeclarativeAvroUpcaster.builder(avroSchema, s.getVersion());
		}
		catch (IOException | RestClientException ex) {
			throw new RuntimeException("unable to retrieve schema for subject " + subject + " version " + version, ex);
		}
	}

	private DeclarativeAvroUpcaster(Schema targetSchema, V targetSchemaVersion, Evaluator evaluator) {
		this.targetSchema = targetSchema;
		this.targetSchemaVersion = targetSchemaVersion;
		this.evaluator = evaluator;
		this.evaluator.setVariable("schema", targetSchema);
	}

	public V getTargetVersion() {
		return targetSchemaVersion;
	}

	@Override
	public GenericRecord upcast(GenericRecord input, V inputVersion) throws UpcasterException {
		this.evaluator.setVariable("input", input);

		GenericRecordBuilder builder = new GenericRecordBuilder(this.targetSchema);

		try {
			for (Schema.Field targetField : this.targetSchema.getFields()) {
				// -- check if there is an expression for this field
				if (this.evaluator.has(targetField.name())) {
					builder.set(targetField, this.evaluator.eval(targetField.name()));
					continue;
				}

				// -- check if the field is in the input
				if (input.getSchema().getField(targetField.name()) != null) {
					builder.set(targetField, input.get(targetField.name()));
					continue;
				}

				// -- check if the field has a default value in the target
				if (targetField.hasDefaultValue()) {
					builder.set(targetField, targetField.defaultVal());
					continue;
				}

				// -- check if the field is optional in the target
				if (targetField.schema().isNullable()) {
					builder.set(targetField, null);
					continue;
				}

				// -- if none of the above worked out, we need to bail
				throw new IllegalStateException("unable to map target field " + targetField.name()
						+ "; no upcaster, input value, default value found and field isn't nullable");
			}

			return builder.build();
		}
		catch (Exception ex) {
			throw new UpcasterException("unable to cast " + input.getSchema().getFullName() + "@" + inputVersion
					+ " to " + targetSchema.getFullName() + "@" + getTargetVersion(), ex);
		}
	}

	public Map<String, String> getExpressions() {
		return this.evaluator.getExpressions();
	}

	public static class Builder<V extends Comparable<V>> {

		private final Schema targetSchema;

		private final V targetVersion;

		private Map<String, String> expressions = new HashMap<>();

		private final EvaluationContext evaluationContext = new StandardEvaluationContext();

		public Builder(Schema targetSchema, V targetVersion) {
			this.targetSchema = targetSchema;
			this.targetVersion = targetVersion;
		}

		public Builder<V> withExpressions(Map<String, String> expressions) {
			this.expressions = expressions;
			return this;
		}

		public Builder<V> withExpression(String fieldName, String expression) {
			this.expressions.put(fieldName, expression);
			return this;
		}

		public Builder<V> withVariable(String name, Object value) {
			this.evaluationContext.setVariable(name, value);
			return this;
		}

		public DeclarativeAvroUpcaster<V> build() {
			return new DeclarativeAvroUpcaster<V>(targetSchema, targetVersion,
					new SpelEvaluator(expressions, evaluationContext));
		}

	}

}
