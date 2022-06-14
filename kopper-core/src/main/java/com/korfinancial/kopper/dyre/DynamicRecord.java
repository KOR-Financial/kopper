/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre;

import org.apache.avro.generic.GenericRecord;

public interface DynamicRecord {

	GenericRecord record();

	Object retrieveDirect(Class<? extends DynamicRecord> declaringClass, String fieldName);

	void manipulateDirect(Class<? extends DynamicRecord> declaringClass, String fieldName, Object value);

}
