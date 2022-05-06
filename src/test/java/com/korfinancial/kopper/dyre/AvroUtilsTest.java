/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.korfinancial.kopper.TestUtils;
import com.korfinancial.kopper.serde.TestModel;

import static org.assertj.core.api.Assertions.assertThat;

class AvroUtilsTest {

	private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

	@Test
	void testGenerateSchema() throws ValueMappingException {
		Schema actualSchema = AvroUtils.schemaFromClass(TestModel.class);

		logger.info("expected: " + TestModel.SCHEMA);
		logger.info("actual: " + actualSchema);

		assertThat(actualSchema.getFullName()).isEqualTo(TestModel.SCHEMA.getFullName());
		assertThat(actualSchema.getType()).isEqualTo(TestModel.SCHEMA.getType());
		assertThat(actualSchema.getFields())
				.containsExactlyInAnyOrder(TestModel.SCHEMA.getFields().toArray(new Schema.Field[] {}));
	}

}
