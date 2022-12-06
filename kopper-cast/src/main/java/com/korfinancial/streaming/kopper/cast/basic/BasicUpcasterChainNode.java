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

package com.korfinancial.streaming.kopper.cast.basic;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterException;
import com.korfinancial.streaming.kopper.cast.VersionedItem;

/**
 * @author Daan Gerits
 */
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
