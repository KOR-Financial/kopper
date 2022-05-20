/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public class UpcasterChain<O, V extends Comparable<V>> {

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

}
