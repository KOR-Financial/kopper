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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.DeclarativeUpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterException;

public class DeclarativeUpcasterTests extends AbstractUpcasterTests {

	private static DeclarativeUpcasterChain<GenericRecord> chain;

	@BeforeAll
	static void beforeAll() throws Exception {
		chain = new DeclarativeUpcasterChain<>();
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

	private static DeclarativeAvroUpcaster upcasterV3() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V3, 3)
				.withExpression("age", "#input.age != null ? #parseInt(#input.age) : null")
				.withVariable("parseInt", Integer.class.getDeclaredMethod("parseInt", String.class)).build();
	}

	private static DeclarativeAvroUpcaster upcasterV4() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V4, 4)
				.withExpression("firstname", "#input.name?.split('\\s', 2)[0] ?: ''")
				.withExpression("lastname", "#input.name?.split('\\s', 2)[1] ?: ''").build();
	}

	private static DeclarativeAvroUpcaster upcasterV5() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V5, 5).withExpression("name",
				"#asRecord(#schema.getField('name').schema(), {firstname: #input.firstname, lastname: #input.lastname})")
				.withVariable("asRecord", AvroHelpers.class.getDeclaredMethod("asRecord", Schema.class, Map.class))
				.build();
	}

}
