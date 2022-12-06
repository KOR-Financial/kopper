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

package com.korfinancial.streaming.kopper.cast.expressions;

import java.util.HashMap;
import java.util.Map;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;

import com.korfinancial.streaming.kopper.cast.avro.GenericRecordPropertyAccessor;

/**
 * @author Daan Gerits
 */
public class SpelEvaluator implements Evaluator {

	private final Map<String, Expression> expressions;

	private final EvaluationContext evaluationContext;

	public SpelEvaluator(Map<String, String> expressions) {
		this(expressions, new StandardEvaluationContext());
	}

	public SpelEvaluator(Map<String, String> expressions, EvaluationContext evaluationContext) {
		this.expressions = new HashMap<>();
		this.evaluationContext = evaluationContext;
		this.evaluationContext.getPropertyAccessors().add(new GenericRecordPropertyAccessor());

		ExpressionParser parser = new SpelExpressionParser();
		expressions.forEach((field, expr) -> this.expressions.put(field, parser.parseExpression(expr)));
	}

	@Override
	public boolean has(String fieldName) {
		return this.expressions.containsKey(fieldName);
	}

	@Override
	public Object eval(String fieldName) {
		return eval(fieldName, Map.of());
	}

	@Override
	public Object eval(String fieldName, Map<String, Object> context) {
		Expression expr = this.expressions.get(fieldName);
		if (expr == null) {
			throw new IllegalArgumentException("no expression for field " + fieldName);
		}

		// -- set the evaluation context
		context.forEach(this.evaluationContext::setVariable);

		// -- evaluate the expression based on the current evaluation context
		Object result = expr.getValue(this.evaluationContext);

		// -- clear the evaluation context
		context.forEach((s, o) -> this.evaluationContext.setVariable(s, null));

		return result;
	}

	@Override
	public void setVariable(String name, @Nullable Object value) {
		evaluationContext.setVariable(name, value);
	}

	public Map<String, Expression> getExpressions() {
		return expressions;
	}

	public EvaluationContext getEvaluationContext() {
		return evaluationContext;
	}

}
