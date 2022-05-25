/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public abstract class AvroHelpers {

	public static GenericRecord asRecord(Schema schema, Map<String, Object> fields) {
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);

		fields.forEach(builder::set);

		return builder.build();
	}

}
