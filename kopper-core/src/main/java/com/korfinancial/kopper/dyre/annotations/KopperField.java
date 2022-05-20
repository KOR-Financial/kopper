/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper.dyre.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface KopperField {

	/**
	 * Indicator to signal if the field is required or not
	 *
	 * defaults to true.
	 * @return true if required, false if not
	 */
	boolean required() default true;

	/**
	 * The type of item within the collection.
	 *
	 * Due to Erasure of Generic types, we are unable to detect the actual type stored
	 * within a generic collection. note: this field is only applicable for collection
	 * fields (List or Map)
	 * @return a Class indicating the type within the collection
	 */
	Class<?> itemType() default Void.class;

}
