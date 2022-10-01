/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;

public class BasicUpcasterChain<O, T extends Upcaster<O, BasicUpcasterContext>>
		implements UpcasterChain<O, BasicUpcasterContext, T> {

	private final String id;

	public static <O, T extends Upcaster<O, BasicUpcasterContext>> Builder<O, T> builder() {
		return new Builder<>(UUID.randomUUID().toString());
	}

	public static <O, T extends Upcaster<O, BasicUpcasterContext>> Builder<O, T> builder(String id) {
		return new Builder<>(id);
	}

	public BasicUpcasterChain() {
		this(UUID.randomUUID().toString());
	}

	public BasicUpcasterChain(String id) {
		this.id = id;
	}

	protected BasicUpcasterChainNode<O> root;

	@Override
	public void registerUpcaster(T upcaster) {
		BasicUpcasterChainNode<O> node = new BasicUpcasterChainNode<>(upcaster);

		if (root == null) {
			root = node;
			return;
		}

		// -- move to the last element
		BasicUpcasterChainNode<O> tail = root;
		while (tail.getNext() != null) {
			tail = tail.getNext();
		}

		tail.setNext(node);
	}

	@Override
	public VersionedItem<O> doUpcast(VersionedItem<O> input) throws UpcasterException {
		// -- return the input if there are no upcasters available
		if (root == null) {
			return input;
		}

		BasicUpcasterContext ctx = new BasicUpcasterContext();

		// -- perform the upcasting
		return root.doUpcast(ctx, input);
	}

	@Override
	public String getId() {
		return id;
	}

	public BasicUpcasterChainNode<O> getRoot() {
		return root;
	}

	public static class Builder<O, T extends Upcaster<O, BasicUpcasterContext>> {

		protected final String id;

		protected final List<T> upcasters;

		public Builder(String id) {
			this.id = id;
			this.upcasters = new ArrayList<>();
		}

		public Builder<O, T> register(T upcaster) {
			this.upcasters.add(upcaster);

			return this;
		}

		public BasicUpcasterChain<O, T> build() {
			BasicUpcasterChain<O, T> chain = new BasicUpcasterChain<>(id);

			upcasters.forEach(chain::registerUpcaster);

			return chain;
		}

	}

}
