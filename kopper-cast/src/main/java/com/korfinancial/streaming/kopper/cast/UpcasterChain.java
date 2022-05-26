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

public class UpcasterChain<O, V extends Comparable<V>> {

	private final String id;

	public static <O, V extends Comparable<V>> Builder<O, V> builder() {
		return new UpcasterChain.Builder<>(UUID.randomUUID().toString());
	}

	public static <O, V extends Comparable<V>> Builder<O, V> builder(String id) {
		return new UpcasterChain.Builder<>(id);
	}

	public UpcasterChain() {
		this(UUID.randomUUID().toString());
	}

	public UpcasterChain(String id) {
		this.id = id;
	}

	protected UpcasterChainNode<O, V> root;

	public void registerUpcaster(Upcaster<O, V> upcaster) {
		UpcasterChainNode<O, V> node = new UpcasterChainNode<>(upcaster);

		if (root == null) {
			root = node;
			return;
		}

		// -- move to the last element
		UpcasterChainNode<O, V> tail = root;
		while (tail.getNext() != null) {
			tail = tail.getNext();
		}

		tail.setNext(node);
	}

	public VersionedItem<O, V> doUpcast(VersionedItem<O, V> input) throws UpcasterException {
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

	public UpcasterChainNode<O, V> getRoot() {
		return root;
	}

	public static class Builder<O, V extends Comparable<V>> {

		protected final String id;

		protected final List<Upcaster<O, V>> upcasters;

		public Builder(String id) {
			this.id = id;
			this.upcasters = new ArrayList<>();
		}

		public Builder<O, V> register(Upcaster<O, V> upcaster) {
			this.upcasters.add(upcaster);

			return this;
		}

		public UpcasterChain<O, V> build() {
			UpcasterChain<O, V> chain = new UpcasterChain<>(id);

			upcasters.forEach(chain::registerUpcaster);

			return chain;
		}

	}

}
