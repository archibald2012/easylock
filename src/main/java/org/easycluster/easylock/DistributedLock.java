package org.easycluster.easylock;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {

	/**
	 * Acquire the lock with supplied resources and want to be the Master. This
	 * method will wait indefinitely for the lock.
	 * 
	 * @throws InterruptedException
	 *             thrown if the current thread is interrupted while waiting
	 */
	void awaitLock() throws InterruptedException;

	/**
	 * Acquire the lock with supplied resources and want to be the Master for
	 * the specified duration of time.
	 * 
	 * @param timeout
	 * @param unit
	 * @return true if be master before the timeout, false if the timeout
	 *         occurred
	 * @throws InterruptedException
	 */
	boolean awaitLock(long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Waits for acquiring to be the master. This method will wait indefinitely
	 * for the lock and will swallow any <code>InterruptedException</code> s
	 * thrown while waiting.
	 */
	void awaitLockUninterruptibly();

	/**
	 * Indicate to acquire the lock and want to be the Master if it is possible.
	 * 
	 * @param callback
	 */
	void lock(LockUpdateCallback callback);

	/**
	 * Releases the lock.
	 */
	void unlock();

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
	String getResource();

	/**
	 * Set the lock manager.
	 * 
	 * @param lockManager
	 */
	void setLockManager(LockManager lockManager);
}
