/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.registry;

import com.korfinancial.streaming.kopper.cast.Upcaster;
import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.UpcasterContext;

public interface UpcasterRegistry<C extends UpcasterContext, UC extends UpcasterChain<?, C, ? extends Upcaster<?, C>>> {

	UC getUpcasters(String subject);

	void registerChain(UC chain);

}
