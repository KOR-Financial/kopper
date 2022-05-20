/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast;

public class UpcasterException extends Exception {

	public UpcasterException() {
	}

	public UpcasterException(String message) {
		super(message);
	}

	public UpcasterException(String message, Throwable cause) {
		super(message, cause);
	}

	public UpcasterException(Throwable cause) {
		super(cause);
	}

}
