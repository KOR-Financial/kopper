/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractUpcasterTests {

	void readV1ToV5(UpcasterChain<GenericRecord, ?, ?> chain) throws UpcasterException {
		VersionedItem<GenericRecord> result = chain.doUpcast(new VersionedItem<>(Payloads.RECORD_V1, 1));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5_WITHOUT_AGE);
	}

	void readV2ToV5(UpcasterChain<GenericRecord, ?, ?> chain) throws UpcasterException {
		VersionedItem<GenericRecord> result = chain.doUpcast(new VersionedItem<>(Payloads.RECORD_V2_WITH_AGE, 2));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

	void readV3ToV5(UpcasterChain<GenericRecord, ?, ?> chain) throws UpcasterException {
		VersionedItem<GenericRecord> result = chain.doUpcast(new VersionedItem<>(Payloads.RECORD_V3, 3));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

	void readV4ToV5(UpcasterChain<GenericRecord, ?, ?> chain) throws UpcasterException {
		VersionedItem<GenericRecord> result = chain.doUpcast(new VersionedItem<>(Payloads.RECORD_V4, 4));

		assertThat(result.getVersion()).isEqualTo(5);
		assertThat(result.getItem()).isEqualTo(Payloads.RECORD_V5);
	}

}
