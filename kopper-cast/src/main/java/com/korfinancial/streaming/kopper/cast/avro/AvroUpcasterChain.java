/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import java.util.UUID;

import org.apache.avro.generic.GenericRecord;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;

public class AvroUpcasterChain extends UpcasterChain<GenericRecord> {

	public static Builder<GenericRecord> builder() {
		return new UpcasterChain.Builder<>(UUID.randomUUID().toString());
	}

	public static Builder<GenericRecord> builder(String id) {
		return new UpcasterChain.Builder<>(id);
	}

}
