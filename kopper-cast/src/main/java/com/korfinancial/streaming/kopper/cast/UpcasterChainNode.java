/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public class UpcasterChainNode<O, V extends Comparable<V>> {

	private final Upcaster<O, V> upcaster;

	private UpcasterChainNode<O, V> next;

	public UpcasterChainNode(Upcaster<O, V> upcaster) {
		this.upcaster = upcaster;
	}

	public VersionedItem<O, V> doUpcast(VersionedItem<O, V> input) throws UpcasterException {
		VersionedItem<O, V> result;

		int cmp = input.getVersion().compareTo(this.upcaster.getTargetVersion());
		if (cmp < 0) {
			// -- the input version is less than the version of the upcaster. This
			// -- means we will need to perform an upcast in order to get to the
			// -- next version.
			result = new VersionedItem<>(this.upcaster.upcast(input.getItem(), input.getVersion()),
					this.upcaster.getTargetVersion());

		}
		else if (cmp > 0) {
			// -- the input is of a later version than the version of the upcaster. In
			// -- this case we just pass the input on to the next in line
			result = input;

		}
		else {
			// -- the version of the input is equal to the version of the upcaster. We
			// -- don't have to do anything in this case except for returning the input.
			result = input;
		}

		// -- proceed to the next upcaster if there is one.
		if (next == null) {
			return result;
		}
		else {
			return next.doUpcast(result);
		}
	}

	public Upcaster<O, V> getUpcaster() {
		return upcaster;
	}

	public UpcasterChainNode<O, V> getNext() {
		return next;
	}

	public V getVersion() {
		return upcaster.getTargetVersion();
	}

	public void setNext(UpcasterChainNode<O, V> next) {
		this.next = next;
	}

}
