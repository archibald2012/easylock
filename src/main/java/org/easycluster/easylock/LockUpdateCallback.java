package org.easycluster.easylock;

public interface LockUpdateCallback {

	/**
	 * 
	 * @param lockId
	 * @param lockStatus
	 * @param updateData
	 */
	void updateLockState(String lockId, LockStatus lockStatus, Object updateData);
}
