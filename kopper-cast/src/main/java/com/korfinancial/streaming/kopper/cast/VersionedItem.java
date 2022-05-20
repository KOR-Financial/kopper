/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public class VersionedItem<O, V extends Comparable<V>> {

	private final O item;

	private final V version;

	public VersionedItem(O item, V version) {
		this.item = item;
		this.version = version;
	}

	public O getItem() {
		return item;
	}

	public V getVersion() {
		return version;
	}

}
