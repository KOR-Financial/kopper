/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde.map.models;

import java.util.Map;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.annotations.KopperField;

public interface MapModel extends DynamicRecord {

	@KopperField(itemType = String.class)
	Map<String, String> getStringMap();

	@KopperField(itemType = Person.class)
	Map<String, Person> getPersonMap();

}
