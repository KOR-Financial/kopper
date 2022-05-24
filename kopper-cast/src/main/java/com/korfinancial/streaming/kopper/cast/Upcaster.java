/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public interface Upcaster<O, V extends Comparable<V>> {

	V getTargetVersion();

	O upcast(O input, V inputVersion) throws UpcasterException;

}
