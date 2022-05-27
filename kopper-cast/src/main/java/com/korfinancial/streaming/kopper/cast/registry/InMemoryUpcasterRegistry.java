/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.registry;

import java.util.HashMap;
import java.util.Map;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;

public class InMemoryUpcasterRegistry implements UpcasterRegistry {

	private final Map<String, UpcasterChain<?>> chains;

	public InMemoryUpcasterRegistry(Map<String, UpcasterChain<?>> chains) {
		this.chains = chains;
	}

	public InMemoryUpcasterRegistry() {
		this(new HashMap<>());
	}

	@Override
	public <T> UpcasterChain<T> getUpcasters(String subject) {
		return (UpcasterChain<T>) chains.get(subject);
	}

	@Override
	public void registerChain(UpcasterChain<?> chain) {
		chains.put(chain.getId(), chain);
	}

}