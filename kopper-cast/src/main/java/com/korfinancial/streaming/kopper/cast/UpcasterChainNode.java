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
		if (this.upcaster.getTargetVersion().compareTo(input.getVersion()) < 0) {
			// -- move ahead in the chain if the input version is lower than the upcaster
			// version
			return this.next.doUpcast(input);
		}

		// -- ask the upcaster to perform the upcast
		O output = this.upcaster.upcast(input.getItem());

		// -- terminate if there is no next element in the chain
		if (next == null) {
			return new VersionedItem<>(output, this.upcaster.getTargetVersion());
		}

		// -- proceed to the next upcaster if there is one. The version is now equal to
		// the version
		// -- of the upcaster
		return this.next.doUpcast(new VersionedItem<>(output, this.upcaster.getTargetVersion()));
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
