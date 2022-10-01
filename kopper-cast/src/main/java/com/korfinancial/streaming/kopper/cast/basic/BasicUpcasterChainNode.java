/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.basic;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;

public class BasicUpcasterChainNode<O> {

	private final Upcaster<O, BasicUpcasterContext> upcaster;

	private BasicUpcasterChainNode<O> next;

	public BasicUpcasterChainNode(Upcaster<O, BasicUpcasterContext> upcaster) {
		this.upcaster = upcaster;
	}

	public VersionedItem<O> doUpcast(BasicUpcasterContext ctx, VersionedItem<O> input) throws UpcasterException {
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
			// -- don't have to do anything in this case except for returning the input.
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

	public Upcaster<O, BasicUpcasterContext> getUpcaster() {
		return upcaster;
	}

	public BasicUpcasterChainNode<O> getNext() {
		return next;
	}

	public Integer getVersion() {
		return upcaster.getTargetVersion();
	}

	public void setNext(BasicUpcasterChainNode<O> next) {
		this.next = next;
	}

}
