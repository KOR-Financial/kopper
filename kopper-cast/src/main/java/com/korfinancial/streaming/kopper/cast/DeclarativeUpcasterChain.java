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

public class DeclarativeUpcasterChain<O>
		implements UpcasterChain<O, DeclarativeUpcasterContext, Upcaster<O, DeclarativeUpcasterContext>> {

	private final String id;

	public static <O> Builder<O> builder() {
		return new Builder<>(UUID.randomUUID().toString());
	}

	public static <O> Builder<O> builder(String id) {
		return new Builder<>(id);
	}

	public DeclarativeUpcasterChain() {
		this(UUID.randomUUID().toString());
	}

	public DeclarativeUpcasterChain(String id) {
		this.id = id;
	}

	protected Node root;

	public void registerUpcaster(Upcaster<O, DeclarativeUpcasterContext> upcaster) {
		Node node = new Node(upcaster);

		if (root == null) {
			root = node;
			return;
		}

		// -- move to the last element
		Node tail = root;
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

		DeclarativeUpcasterContext ctx = new DeclarativeUpcasterContext();

		// -- perform the upcasting
		return root.doUpcast(ctx, input);
	}

	public String getId() {
		return id;
	}

	class Node {

		private final Upcaster<O, DeclarativeUpcasterContext> upcaster;

		private Node next;

		Node(Upcaster<O, DeclarativeUpcasterContext> upcaster) {
			this.upcaster = upcaster;
		}

		VersionedItem<O> doUpcast(DeclarativeUpcasterContext ctx, VersionedItem<O> input) throws UpcasterException {
			VersionedItem<O> result;

			int cmp = input.getVersion().compareTo(this.upcaster.getTargetVersion());
			if (cmp < 0) {
				// -- the input version is less than the version of the upcaster. This
				// -- means we will need to perform an upcast in order to get to the
				// -- next version.
				result = new VersionedItem<>(this.upcaster.upcast(ctx, input.getItem(), input.getVersion()),
						this.upcaster.getTargetVersion());

			}
			else if (cmp > 0) {
				// -- the input is of a later version than the version of the upcaster. In
				// -- this case we just pass the input on to the next in line
				result = input;

			}
			else {
				// -- the version of the input is equal to the version of the upcaster. We
				// -- don't have to do anything in this case except for returning the
				// input.
				result = input;
			}

			// -- proceed to the next upcaster if there is one.
			if (next == null) {
				return result;
			}
			else {
				return next.doUpcast(ctx, result);
			}
		}

		Upcaster<O, DeclarativeUpcasterContext> getUpcaster() {
			return upcaster;
		}

		Node getNext() {
			return next;
		}

		Integer getVersion() {
			return upcaster.getTargetVersion();
		}

		void setNext(Node next) {
			this.next = next;
		}

	}

	public static class Builder<O> {

		protected final String id;

		protected final List<Upcaster<O, DeclarativeUpcasterContext>> upcasters;

		public Builder(String id) {
			this.id = id;
			this.upcasters = new ArrayList<>();
		}

		public Builder<O> register(Upcaster<O, DeclarativeUpcasterContext> upcaster) {
			this.upcasters.add(upcaster);

			return this;
		}

		public DeclarativeUpcasterChain<O> build() {
			DeclarativeUpcasterChain<O> chain = new DeclarativeUpcasterChain<>(id);

			upcasters.forEach(chain::registerUpcaster);

			return chain;
		}

	}

}
