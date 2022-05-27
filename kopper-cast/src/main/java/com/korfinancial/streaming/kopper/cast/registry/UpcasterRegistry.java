/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.registry;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;

public interface UpcasterRegistry {

	<T> UpcasterChain<T> getUpcasters(String subject);

	void registerChain(UpcasterChain<?> chain);

}
