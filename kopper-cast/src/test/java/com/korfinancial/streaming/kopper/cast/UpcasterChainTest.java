/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UpcasterChainTest {

	private static final PropertiesUpcaster pu1 = new PropertiesUpcaster(2, Map.of("username", "user"));

	private static final PropertiesUpcaster pu2 = new PropertiesUpcaster(3, Map.of("description", "description"));

	private static final PropertiesUpcaster pu3 = new PropertiesUpcaster(5, Map.of("name", "name"));

	private static final PropertiesUpcaster pu4 = new PropertiesUpcaster(6, Map.of("date", "date"));

	private static final UpcasterChain<Properties, Integer> chain = new UpcasterChain<>();

	@BeforeAll
	static void beforeAll() {
		chain.registerUpcaster(pu1);
		chain.registerUpcaster(pu2);
		chain.registerUpcaster(pu3);
		chain.registerUpcaster(pu4);
	}

	@Test
	void testChainConstruction() {
		// -- check the upcasters are there
		UpcasterChainNode<Properties, Integer> current = chain.root;
		assertThat(current.getVersion()).isEqualTo(2);
		assertThat(current.getUpcaster()).isEqualTo(pu1);
		assertThat(current.getNext()).isNotNull();

		current = current.getNext();
		assertThat(current.getVersion()).isEqualTo(3);
		assertThat(current.getUpcaster()).isEqualTo(pu2);
		assertThat(current.getNext()).isNotNull();

		current = current.getNext();
		assertThat(current.getVersion()).isEqualTo(5);
		assertThat(current.getUpcaster()).isEqualTo(pu3);
		assertThat(current.getNext()).isNotNull();

		current = current.getNext();
		assertThat(current.getVersion()).isEqualTo(6);
		assertThat(current.getUpcaster()).isEqualTo(pu4);
		assertThat(current.getNext()).isNull();
	}

	@Test
	void testUpcastingFromRoot() throws UpcasterException {
		Properties data = new Properties();
		data.setProperty("id", "ID");

		VersionedItem<Properties, Integer> result = chain.doUpcast(new VersionedItem<>(data, 1));

		assertThat(result.getVersion()).isEqualTo(6);
		assertThat(result.getItem()).isNotNull();
		assertThat(result.getItem()).containsOnlyKeys("id", "username", "description", "name", "date");
	}

	@Test
	void testUpcastingFromInbetween() throws UpcasterException {
		Properties data = new Properties();
		data.setProperty("id", "ID");
		data.setProperty("username", "luser");
		data.setProperty("description", "description");

		VersionedItem<Properties, Integer> result = chain.doUpcast(new VersionedItem<>(data, 4));

		assertThat(result.getVersion()).isEqualTo(6);
		assertThat(result.getItem()).isNotNull();
		assertThat(result.getItem()).containsOnlyKeys("id", "username", "description", "name", "date");
	}

	@Test
	void testUpcastingFromTail() throws UpcasterException {
		Properties data = new Properties();
		data.setProperty("id", "ID");
		data.setProperty("username", "luser");
		data.setProperty("description", "description");
		data.setProperty("name", "name");
		data.setProperty("date", "date");

		VersionedItem<Properties, Integer> result = chain.doUpcast(new VersionedItem<>(data, 6));

		assertThat(result.getVersion()).isEqualTo(6);
		assertThat(result.getItem()).isNotNull();
		assertThat(result.getItem()).containsOnlyKeys("id", "username", "description", "name", "date");
	}

	static class PropertiesUpcaster implements Upcaster<Properties, Integer> {

		private final Integer targetVersion;

		private final Map<String, String> changes;

		PropertiesUpcaster(Integer targetVersion, Map<String, String> changes) {
			this.targetVersion = targetVersion;
			this.changes = changes;
		}

		@Override
		public Integer getTargetVersion() {
			return targetVersion;
		}

		@Override
		public Properties upcast(Properties input) {
			changes.forEach(input::setProperty);

			return input;
		}

	}

}
