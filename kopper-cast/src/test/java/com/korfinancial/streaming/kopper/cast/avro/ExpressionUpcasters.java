/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import java.util.Map;

public abstract class ExpressionUpcasters {

	// @formatter-off

	public static GenericRecordUpcaster V1_TO_V2_UPCASTER = new ExpressionBasedGenericRecordUpcaster(Schemas.SCHEMA_V2, 2, Map.of());

	public static GenericRecordUpcaster V2_TO_V3_UPCASTER = new ExpressionBasedGenericRecordUpcaster(Schemas.SCHEMA_V3, 3, Map.of(
		"age", "#input.age != null ? #parseInt(#input.age) : null"
	));

	public static GenericRecordUpcaster V3_TO_V4_UPCASTER = new ExpressionBasedGenericRecordUpcaster(Schemas.SCHEMA_V4, 4, Map.of(
		"firstname", "#input.name?.split('\\s', 2)[0] ?: ''",
		"lastname", "#input.name?.split('\\s', 2)[1] ?: ''"
	));

	public static GenericRecordUpcaster V4_TO_V5_UPCASTER = new ExpressionBasedGenericRecordUpcaster(Schemas.SCHEMA_V5, 5, Map.of(
		"name", "#asRecord(#schema.getField('name').schema(), {firstname: #input.firstname, lastname: #input.lastname})"
	));

	// @formatter-on
}
