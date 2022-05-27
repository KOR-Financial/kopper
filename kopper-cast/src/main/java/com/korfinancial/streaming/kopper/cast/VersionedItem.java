/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public class VersionedItem<O> {

	private final O item;

	private final Integer version;

	public VersionedItem(O item, Integer version) {
		this.item = item;
		this.version = version;
	}

	public O getItem() {
		return item;
	}

	public Integer getVersion() {
		return version;
	}

}
