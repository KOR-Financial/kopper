/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.expressions;

import java.util.Map;

import org.springframework.lang.Nullable;

public interface Evaluator {

	boolean has(String fieldName);

	Object eval(String fieldName);

	Object eval(String fieldName, Map<String, Object> context);

	void setVariable(String name, @Nullable Object value);

	Map<String, String> getExpressions();

}
