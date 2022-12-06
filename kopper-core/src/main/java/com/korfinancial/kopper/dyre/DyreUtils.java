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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import com.google.common.base.CaseFormat;

import com.korfinancial.kopper.dyre.annotations.KopperField;

/**
 * @author Daan Gerits
 */
public abstract class DyreUtils {

	public static <T extends Annotation> T annotation(Annotation[] annotations, Class<T> annoCls) {
		for (Annotation anno : annotations) {
			if (annoCls.isAssignableFrom(anno.annotationType())) {
				return (T) anno;
			}
		}

		return null;
	}

	public static <T> T expectType(Class<T> expected, Object obj) throws ValueMappingException {
		if (obj == null) {
			return null;
		}

		if (!expected.isAssignableFrom(obj.getClass())) {
			throw new ValueMappingException(
					"expected a " + expected.getName() + " but received a " + obj.getClass().getName());
		}

		return (T) obj;
	}

	public static KopperField kopperField(Class<?> cls, boolean required) {
		return new KopperField() {

			@Override
			public boolean required() {
				return required;
			}

			@Override
			public Class<? extends Annotation> annotationType() {
				return KopperField.class;
			}
		};
	}

	public static String getFieldName(Method method, String prefix) {
		String field = method.getName().substring(prefix.length());

		// -- change the field from camel case to snake case
		return getFieldName(field);
	}

	public static String getFieldName(String input) {
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, input);
	}

}
