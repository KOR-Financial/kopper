/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.korfinancial.kopper.dyre.DynamicRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class StateStoreTestUtils {

	private static final Logger logger = LoggerFactory.getLogger(StateStoreTestUtils.class);

	public static <T> Map<String, T> rollup(Iterator<ConsumerRecord<String, T>> iter) {
		Map<String, T> result = new HashMap<>();

		while (iter.hasNext()) {
			ConsumerRecord<String, T> record = iter.next();

			if (record.value() == null) {
				result.remove(record.key());
				logger.debug("key " + record.key() + " removed");
			}
			else {
				result.put(record.key(), record.value());
				logger.debug("key " + record.key() + " set to " + record.value());
			}
		}

		return result;
	}

	public static <T extends DynamicRecord> void assertContains(ReadOnlyKeyValueStore<String, T> store,
			Map<String, T> expected) {
		int count = 0;
		KeyValueIterator<String, T> iter = store.all();
		while (iter.hasNext()) {
			KeyValue<String, T> kv = iter.next();
			count++;

			T expectedValue = expected.get(kv.key);
			if ((expectedValue == null && kv.value != null) || (expectedValue != null && kv.value == null)) {
				fail("expected value and store value are not both null or both set");
			}

			if (expectedValue != null && (!expectedValue.record().equals(kv.value.record()))) {
				fail("expected value not equal to the value from the store");
			}
		}

		assertThat(expected).withFailMessage(
				"expected number of records (" + expected.size() + ") != (" + count + ") store's number of records")
				.hasSize(count);
	}

}
