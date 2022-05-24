/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import com.korfinancial.streaming.kopper.cast.UpcasterException;

import com.korfinancial.streaming.kopper.cast.expressions.AvroFunctions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.HashMap;
import java.util.Map;

public class ExpressionBasedGenericRecordUpcaster extends GenericRecordUpcaster {
	private final Map<String, Expression> expressions;

	private final EvaluationContext evaluationContext;

	public ExpressionBasedGenericRecordUpcaster(Schema targetSchema, Integer targetSchemaVersion, Map<String, String> expressions) {
		this(targetSchema, targetSchemaVersion, expressions, new StandardEvaluationContext());
	}

	public ExpressionBasedGenericRecordUpcaster(Schema targetSchema, Integer targetSchemaVersion, Map<String, String> expressions, EvaluationContext evaluationContext) {
		super(targetSchema, targetSchemaVersion);

		ExpressionParser parser = new SpelExpressionParser();

		this.expressions = new HashMap<>();
		expressions.forEach((field, expr) ->
			this.expressions.put(field, parser.parseExpression(expr)));

		this.evaluationContext = evaluationContext;
		this.evaluationContext.getPropertyAccessors().add(new GenericRecordPropertyAccessor());

		this.evaluationContext.setVariable("schema", getTargetSchema());

		registerCustomFunctions(this.evaluationContext);
	}

	@Override
	public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) throws UpcasterException {
		this.evaluationContext.setVariable("input", input);

		for (Map.Entry<String, Expression> entry : expressions.entrySet()) {
			try {
				builder.set(entry.getKey(), entry.getValue().getValue(this.evaluationContext));
			} catch (Exception ex) {
				throw new UpcasterException("unable to process expression " + entry.getKey() + "(" + entry.getValue().getExpressionString() + ")", ex);
			}
		}

		return builder;
	}

	protected void registerCustomFunctions(EvaluationContext ctx) throws UpcasterException {
		try {
			ctx.setVariable("parseInt", Integer.class.getDeclaredMethod("parseInt", String.class));
			ctx.setVariable("asRecord", AvroFunctions.class.getDeclaredMethod("asRecord", Schema.class, Map.class));
		}
		catch (NoSuchMethodException e) {
			throw new UpcasterException(e);
		}
	}
}
