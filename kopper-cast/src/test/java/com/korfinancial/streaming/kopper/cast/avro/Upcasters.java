/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public abstract class Upcasters {

	public static GenericRecordUpcaster V1_TO_V2_UPCASTER = new GenericRecordUpcaster(Schemas.SCHEMA_V2, 2) {
		@Override
		public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
			return builder;
		}
	};

	public static GenericRecordUpcaster V2_TO_V3_UPCASTER = new GenericRecordUpcaster(Schemas.SCHEMA_V3, 3) {
		@Override
		public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
			// -- check if the age field is on the input
			if (input.hasField("age")) {
				Object originalValue = input.get("age");
				if (originalValue instanceof String s) {
					builder.set("age", Integer.parseInt(s));
				}
			}

			return builder;
		}
	};

	public static GenericRecordUpcaster V3_TO_V4_UPCASTER = new GenericRecordUpcaster(Schemas.SCHEMA_V4, 4) {
		@Override
		public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
			// -- check if the name was set
			if (input.hasField("name")) {
				builder.set("firstname", "");
				builder.set("lastname", "");

				Object originalValue = input.get("name");
				if (originalValue instanceof String s) {
					String[] parts = s.split("\\s", 2);

					if (parts.length >= 1) {
						builder.set("firstname", parts[0]);
					}

					if (parts.length >= 2) {
						builder.set("lastname", parts[1]);
					}
				}
			}

			return builder;
		}
	};

	public static GenericRecordUpcaster V4_TO_V5_UPCASTER = new GenericRecordUpcaster(Schemas.SCHEMA_V5, 5) {
		@Override
		public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
			// -- check if the name was set
			builder.set("name", new GenericRecordBuilder(Schemas.SCHEMA_V5.getField("name").schema())
					.set("firstname", input.get("firstname"))
					.set("lastname", input.get("lastname"))
				.build());

			return builder;
		}
	};

}
