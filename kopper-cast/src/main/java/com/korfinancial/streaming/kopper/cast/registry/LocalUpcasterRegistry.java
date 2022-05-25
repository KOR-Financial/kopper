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

public class LocalUpcasterRegistry implements UpcasterRegistry {

	private final Map<String, UpcasterChain<?, Integer>> chains;

	public LocalUpcasterRegistry(Map<String, UpcasterChain<?, Integer>> chains) {
		this.chains = chains;
	}

	public LocalUpcasterRegistry() {
		this(new HashMap<>());
	}

	@Override
	public <T> UpcasterChain<T, Integer> getUpcasters(String subject) {
		return (UpcasterChain<T, Integer>) chains.get(subject);
	}

	@Override
	public void registerChain(UpcasterChain<?, Integer> chain) {
		chains.put(chain.getId(), chain);
	}

}
