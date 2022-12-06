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
