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

package com.korfinancial.streaming.kopper.cast.registry;

import java.util.HashMap;
import java.util.Map;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterContext;

/**
 * @author Daan Gerits
 */
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
