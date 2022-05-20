/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre.decoders;

import java.lang.annotation.Annotation;

import com.korfinancial.kopper.dyre.ValueMappingException;

public interface ValueDecoder {

	ValueDecoder DEFAULT_DECODER = new DefaultValueDecoder();

	Object decode(Class<?> expected, Object actualValue, Annotation[] annotations) throws ValueMappingException;

}
