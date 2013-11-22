package org.easycluster.easylock;

public interface LockManager {

	/**
	 * Indicate to acquire the lock with supplied resources and want to be the
	 * Master if it is possible.
	 * 
	 * @param lockResource
	 * @param callback
	 */
	void acquireLock(String lockResource, LockUpdateCallback callback);

	/**
	 * Indicate to release the Master lock if this instance is holding on it. It
	 * will return the latest lock status for this instance.
	 * 
	 * @param lockId
	 * @param notify
	 *            
	 */
	void releaseLock(String lockId, boolean notify);
}
