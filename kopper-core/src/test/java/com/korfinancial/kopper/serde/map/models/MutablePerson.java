/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.serde.map.models;

import java.util.List;
import java.util.Map;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.DynamicRecords;
import com.korfinancial.kopper.dyre.annotations.KopperField;
import com.korfinancial.kopper.dyre.annotations.KopperRecord;

@KopperRecord
public interface MutablePerson extends DynamicRecord {

	static MutablePerson create(String name, State state, List<MutablePerson> siblings) {
		return DynamicRecords.newRecord(MutablePerson.class,
				Map.of("name", name, "state", state, "siblings", siblings));
	}

	String getName();

	void setName(String name);

	@KopperField(itemType = MutablePerson.class)
	List<MutablePerson> getSiblings();

	void setSiblings(@KopperField(itemType = MutablePerson.class) List<MutablePerson> siblings);

	State getState();

	void setState(State state);

}
