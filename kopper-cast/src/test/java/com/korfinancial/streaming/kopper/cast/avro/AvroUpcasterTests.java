/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterChain;
import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterContext;

public class AvroUpcasterTests extends AbstractUpcasterTests {

	private static BasicUpcasterChain<GenericRecord, Upcaster<GenericRecord, BasicUpcasterContext>> chain;

	@BeforeAll
	static void beforeAll() {
		chain = new BasicUpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());
		chain.registerUpcaster(upcasterV5());
	}

	@Test
	void readV1ToV5() throws UpcasterException {
		readV1ToV5(chain);
	}

	@Test
	void readV2ToV5() throws UpcasterException {
		readV2ToV5(chain);
	}

	@Test
	void readV3ToV5() throws UpcasterException {
		readV3ToV5(chain);
	}

	@Test
	void readV4ToV5() throws UpcasterException {
		readV4ToV5(chain);
	}

	private static AvroUpcaster upcasterV3() {
		return new AvroUpcaster(Schemas.SCHEMA_V3, 3) {
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
	}

	private static AvroUpcaster upcasterV4() {
		return new AvroUpcaster(Schemas.SCHEMA_V4, 4) {
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
	}

	private static AvroUpcaster upcasterV5() {
		return new AvroUpcaster(Schemas.SCHEMA_V5, 5) {
			@Override
			public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
				// -- check if the name was set
				builder.set("name", new GenericRecordBuilder(Schemas.SCHEMA_V5.getField("name").schema())
						.set("firstname", input.get("firstname")).set("lastname", input.get("lastname")).build());

				return builder;
			}
		};
	}

}
