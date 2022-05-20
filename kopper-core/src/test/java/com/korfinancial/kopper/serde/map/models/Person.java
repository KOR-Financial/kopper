/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde.map.models;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.annotations.KopperRecord;

@KopperRecord
public interface Person extends DynamicRecord {

	String getName();

	Integer getAge();

	Boolean isMarried();

}
