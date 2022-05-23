/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.avro;

import org.apache.avro.generic.GenericRecord;

public class GenericRecordWrapper {
	private final GenericRecord subject;

	public GenericRecordWrapper(GenericRecord subject) {
		this.subject = subject;
	}

	public GenericRecord getSubject() {
		return subject;
	}
}
