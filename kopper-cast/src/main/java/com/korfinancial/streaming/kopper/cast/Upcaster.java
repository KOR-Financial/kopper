/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public interface Upcaster<O, C extends UpcasterContext> {

	Integer getTargetVersion();

	O upcast(C ctx, O input, Integer inputVersion) throws UpcasterException;

}
