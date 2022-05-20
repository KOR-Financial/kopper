/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre.encoders;

import java.lang.annotation.Annotation;

import org.apache.avro.Schema;

import com.korfinancial.kopper.dyre.ValueMappingException;

public interface ValueEncoder {

	ValueEncoder DEFAULT_ENCODER = new DefaultValueEncoder();

	Object encode(Schema schema, Object actualValue, Annotation[] annotations) throws ValueMappingException;

}
