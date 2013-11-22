package org.easycluster.easylock;

public interface LockUpdateCallback {

	void updateLockState(String lockId, LockStatus lockStatus);
}
