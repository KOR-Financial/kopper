/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.registry;

import java.util.HashMap;
import java.util.Map;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterContext;

public class InMemoryUpcasterRegistry<C extends UpcasterContext, UC extends UpcasterChain<?, C, ? extends Upcaster<?, C>>>
		implements UpcasterRegistry<C, UC> {

	private final Map<String, UC> chains;

	public InMemoryUpcasterRegistry(Map<String, UC> chains) {
		this.chains = chains;
	}

	public InMemoryUpcasterRegistry() {
		this(new HashMap<>());
	}

	@Override
	public UC getUpcasters(String subject) {
		return chains.get(subject);
	}

	@Override
	public void registerChain(UC chain) {
		chains.put(chain.getId(), chain);
	}

}
