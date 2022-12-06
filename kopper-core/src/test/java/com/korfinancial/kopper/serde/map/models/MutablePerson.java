/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.korfinancial.kopper.serde.map.models;

import java.util.List;
import java.util.Map;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.dyre.DynamicRecords;
import com.korfinancial.kopper.dyre.annotations.KopperRecord;

@KopperRecord
public interface MutablePerson extends DynamicRecord {

	static MutablePerson create(String name, State state, List<MutablePerson> siblings) {
		return DynamicRecords.getInstance().newRecord(MutablePerson.class,
				Map.of("name", name, "state", state, "siblings", siblings));
	}

	String getName();

	void setName(String name);

	List<MutablePerson> getSiblings();

	void setSiblings(List<MutablePerson> siblings);

	State getState();

	void setState(State state);

}
