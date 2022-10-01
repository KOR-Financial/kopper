/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterChain;
import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterChainNode;
import com.korfinancial.streaming.kopper.cast.basic.BasicUpcasterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BasicUpcasterChainTest {

	private PropertiesUpcaster pu1;

	private PropertiesUpcaster pu2;

	private PropertiesUpcaster pu3;

	private PropertiesUpcaster pu4;

	private BasicUpcasterChain<Properties, ?> chain;

	@BeforeEach
	void beforeAll() {
		pu1 = spy(new PropertiesUpcaster(2, Map.of("username", "user")));
		pu2 = spy(new PropertiesUpcaster(3, Map.of("description", "description")));
		pu3 = spy(new PropertiesUpcaster(5, Map.of("name", "name")));
		pu4 = spy(new PropertiesUpcaster(6, Map.of("date", "date")));

		// @formatter-off
		chain = BasicUpcasterChain.<Properties, Upcaster<Properties, BasicUpcasterContext>>builder().register(pu1)
				.register(pu2).register(pu3).register(pu4).build();
		// @formatter-on
	}

	@Test
	void testChainConstruction() {
		// -- check the upcasters are there
		BasicUpcasterChainNode<Properties> current = chain.getRoot();
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

		VersionedItem<Properties> result = chain.doUpcast(new VersionedItem<>(data, 1));

		verify(pu1).upcast(any(), any(), anyInt());
		verify(pu2).upcast(any(), any(), anyInt());
		verify(pu3).upcast(any(), any(), anyInt());
		verify(pu4).upcast(any(), any(), anyInt());

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

		VersionedItem<Properties> result = chain.doUpcast(new VersionedItem<>(data, 4));

		verify(pu1, never()).upcast(any(), any(), anyInt());
		verify(pu2, never()).upcast(any(), any(), anyInt());
		verify(pu3).upcast(any(), any(), anyInt());
		verify(pu4).upcast(any(), any(), anyInt());

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

		VersionedItem<Properties> result = chain.doUpcast(new VersionedItem<>(data, 6));

		verify(pu1, never()).upcast(any(), any(), anyInt());
		verify(pu2, never()).upcast(any(), any(), anyInt());
		verify(pu3, never()).upcast(any(), any(), anyInt());
		verify(pu4, never()).upcast(any(), any(), anyInt());

		assertThat(result.getVersion()).isEqualTo(6);
		assertThat(result.getItem()).isNotNull();
		assertThat(result.getItem()).containsOnlyKeys("id", "username", "description", "name", "date");
	}

	static class PropertiesUpcaster implements Upcaster<Properties, BasicUpcasterContext> {

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
		public Properties upcast(BasicUpcasterContext ctx, Properties input, Integer inputVersion) {
			changes.forEach(input::setProperty);

			return input;
		}

	}

}
