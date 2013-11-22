package org.easycluster.easylock.zookeeper;

import org.easycluster.easylock.DefaultDistributedLock;
import org.easycluster.easylock.LockManager;

public class ZooKeeperDistributedLock extends DefaultDistributedLock {

	public ZooKeeperDistributedLock(final String lockResource, final String zooKeeperConnectString, final int zooKeeperSessionTimeoutMillis,
			final String lockRootNode) {
		super(lockResource);
		LockManager lockManager = new ZooKeeperLockManager(zooKeeperConnectString, zooKeeperSessionTimeoutMillis, lockRootNode);
		setLockManager(lockManager);
	}
}
