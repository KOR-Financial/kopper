/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public interface UpcasterChain<O, C extends UpcasterContext, T extends Upcaster<O, C>> {
	void registerUpcaster(T upcaster);

	VersionedItem<O> doUpcast(VersionedItem<O> input) throws UpcasterException;

	String getId();
}
