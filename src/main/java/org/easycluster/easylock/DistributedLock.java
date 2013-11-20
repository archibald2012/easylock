package org.easycluster.easylock;

import java.util.concurrent.Future;

public interface DistributedLock {

	/**
	 * Acquires the lock.
	 * 
	 * @return the id of the lock
	 * 
	 */
	Future<String> lock();

	/**
	 * Releases the lock.
	 * 
	 * @param lockId
	 *            the id of the lock
	 */
	void unlock(String lockId);

	/**
	 * Return the current lock status.
	 * 
	 * @return
	 */
	LockStatus getStatus();

	/**
	 * Return the lock resource.
	 * 
	 * @return
	 */
	String getLockResource();
}
