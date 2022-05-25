/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;

import static org.assertj.core.api.Assertions.assertThat;

public class DeclarativeUpcasterTests {

	@Test
	void readV1ToV2WithoutUpcaster() throws UpcasterException {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();

		GenericRecord v1 = Payloads.RECORD_V1;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v1, 1));
		assertThat(result.getVersion()).isEqualTo(1);

		// -- make sure the schema we read is compatible with SCHEMA_V2
		SchemaCompatibility.SchemaPairCompatibility compatibility = SchemaCompatibility
				.checkReaderWriterCompatibility(Schemas.SCHEMA_V2, result.getItem().getSchema());
		assertThat(compatibility.getResult()).isEqualTo(SchemaCompatibility.SchemaCompatibilityResult.compatible());
	}

	// == V3 ==================================================================

	@Test
	void readV1ToV3() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());

		GenericRecord v1 = Payloads.RECORD_V1;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v1, 1));
		assertThat(result.getVersion()).isEqualTo(3);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V3_WITHOUT_AGE);
	}

	@Test
	void readV2ToV3() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());

		GenericRecord v2 = Payloads.RECORD_V2_WITH_AGE;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v2, 2));

		assertThat(result.getVersion()).isEqualTo(3);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V3);
	}

	// == V4 ==================================================================

	@Test
	void readV1ToV4() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());

		GenericRecord v1 = Payloads.RECORD_V1;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v1, 1));

		assertThat(result.getVersion()).isEqualTo(4);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V4_WITHOUT_AGE);
	}

	@Test
	void readV2ToV4() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());

		GenericRecord v2 = Payloads.RECORD_V2_WITH_AGE;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v2, 2));

		assertThat(result.getVersion()).isEqualTo(4);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V4);
	}

	@Test
	void readV3ToV4() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());

		GenericRecord v3 = Payloads.RECORD_V3;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v3, 3));

		assertThat(result.getVersion()).isEqualTo(4);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V4);
	}

	// == V4 ==================================================================

	@Test
	void readV1ToV5() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());
		chain.registerUpcaster(upcasterV5());

		GenericRecord v1 = Payloads.RECORD_V1;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v1, 1));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5_WITHOUT_AGE);
	}

	@Test
	void readV2ToV5() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());
		chain.registerUpcaster(upcasterV5());

		GenericRecord v2 = Payloads.RECORD_V2_WITH_AGE;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v2, 2));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

	@Test
	void readV3ToV5() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());
		chain.registerUpcaster(upcasterV5());

		GenericRecord v3 = Payloads.RECORD_V3;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v3, 3));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

	@Test
	void readV4ToV5() throws Exception {
		UpcasterChain<GenericRecord, Integer> chain = new UpcasterChain<>();
		chain.registerUpcaster(upcasterV3());
		chain.registerUpcaster(upcasterV4());
		chain.registerUpcaster(upcasterV5());

		GenericRecord v4 = Payloads.RECORD_V4;
		VersionedItem<GenericRecord, Integer> result = chain.doUpcast(new VersionedItem<>(v4, 4));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

	private DeclarativeAvroUpcaster<Integer> upcasterV3() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V3, 3)
				.withExpression("age", "#input.age != null ? #parseInt(#input.age) : null")
				.withVariable("parseInt", Integer.class.getDeclaredMethod("parseInt", String.class)).build();
	}

	private DeclarativeAvroUpcaster<Integer> upcasterV4() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V4, 4)
				.withExpression("firstname", "#input.name?.split('\\s', 2)[0] ?: ''")
				.withExpression("lastname", "#input.name?.split('\\s', 2)[1] ?: ''").build();
	}

	private DeclarativeAvroUpcaster<Integer> upcasterV5() throws NoSuchMethodException {
		return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V5, 5).withExpression("name",
				"#asRecord(#schema.getField('name').schema(), {firstname: #input.firstname, lastname: #input.lastname})")
				.withVariable("asRecord", AvroHelpers.class.getDeclaredMethod("asRecord", Schema.class, Map.class))
				.build();
	}

}
