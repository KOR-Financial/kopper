/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UpcasterChain<O> {

	private final String id;

	public static <O> Builder<O> builder() {
		return new UpcasterChain.Builder<>(UUID.randomUUID().toString());
	}

	public static <O> Builder<O> builder(String id) {
		return new UpcasterChain.Builder<>(id);
	}

	public UpcasterChain() {
		this(UUID.randomUUID().toString());
	}

	public UpcasterChain(String id) {
		this.id = id;
	}

	protected UpcasterChainNode<O> root;

	public void registerUpcaster(Upcaster<O> upcaster) {
		UpcasterChainNode<O> node = new UpcasterChainNode<>(upcaster);

		if (root == null) {
			root = node;
			return;
		}

		// -- move to the last element
		UpcasterChainNode<O> tail = root;
		while (tail.getNext() != null) {
			tail = tail.getNext();
		}

		tail.setNext(node);
	}

	public VersionedItem<O> doUpcast(VersionedItem<O> input) throws UpcasterException {
		// -- return the input if there are no upcasters available
		if (root == null) {
			return input;
		}

		// -- perform the upcasting
		return root.doUpcast(input);
	}

	public String getId() {
		return id;
	}

	public static class Builder<O> {

		protected final String id;

		protected final List<Upcaster<O>> upcasters;

		public Builder(String id) {
			this.id = id;
			this.upcasters = new ArrayList<>();
		}

		public Builder<O> register(Upcaster<O> upcaster) {
			this.upcasters.add(upcaster);

			return this;
		}

		public UpcasterChain<O> build() {
			UpcasterChain<O> chain = new UpcasterChain<>(id);

			upcasters.forEach(chain::registerUpcaster);

			return chain;
		}

	}

}
