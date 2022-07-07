/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde.map.models;

import java.util.Map;

import com.korfinancial.kopper.dyre.DynamicRecord;

public interface MapModel extends DynamicRecord {

	Map<String, String> getStringMap();

	Map<String, Person> getPersonMap();

}
