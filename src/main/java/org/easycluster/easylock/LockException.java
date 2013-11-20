package org.easycluster.easylock;

public class LockException extends RuntimeException {

	private static final long	serialVersionUID	= 1L;

	/**
	 * @param message
	 * @param cause
	 */
	public LockException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public LockException(String message) {
		super(message);
	}

}
