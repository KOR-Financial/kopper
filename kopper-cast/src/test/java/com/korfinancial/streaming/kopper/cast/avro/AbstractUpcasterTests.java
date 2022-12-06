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
