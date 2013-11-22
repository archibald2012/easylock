package org.easycluster.easylock.zookeeper;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.easycluster.easylock.LockStatus;
import org.easycluster.easylock.LockUpdateCallback;
import org.easycluster.easylock.zookeeper.ZooKeeperLockManager;
import org.junit.Test;

public class ZooKeeperLockManagerTest {

	@Test
	public void testReleaseLock() throws Exception {
		final String lockResource = UUID.randomUUID().toString();

		final ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");

		final CountDownLatch latch = new CountDownLatch(1);

		lockManager.acquireLock(lockResource, new LockUpdateCallback() {

			@Override
			public void updateLockState(String lockId, LockStatus lockStatus) {
				lockManager.releaseLock(lockId, false);
				lockManager.releaseLock(lockId, false);
				lockManager.releaseLock(lockId, false);
				latch.countDown();
			}
		});

		latch.await();
	}

	@Test
	public void testAcquiredLock() throws Exception {
		final String lockResource = UUID.randomUUID().toString();

		final ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");

		final CountDownLatch latch = new CountDownLatch(50);

		final LockUpdateCallback callback = new LockUpdateCallback() {

			@Override
			public void updateLockState(String lockId, LockStatus lockStatus) {
				if (LockStatus.MASTER == lockStatus) {
					lockManager.releaseLock(lockId, false);
					latch.countDown();
				}
			}
		};

		for (int i = 0; i < 50; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					lockManager.acquireLock(lockResource, callback);
				}
			}).start();
		}

		latch.await();
	}

	//@Test
	public void testAcquire() throws Exception {

		final String lockResource = UUID.randomUUID().toString();

		final ZooKeeperLockManager mutexLock1 = new ZooKeeperLockManager("127.0.0.1:2181");
		mutexLock1.setLockInstance("host1");
		final ZooKeeperLockManager mutexLock2 = new ZooKeeperLockManager("127.0.0.1:2181");
		mutexLock2.setLockInstance("host2");
		final ZooKeeperLockManager mutexLock3 = new ZooKeeperLockManager("127.0.0.1:2181");
		mutexLock3.setLockInstance("host3");

		final CountDownLatch latch = new CountDownLatch(30);

		new Thread(new Runnable() {

			@Override
			public void run() {
				mutexLock1.acquireLock(lockResource, new LockUpdateCallback() {

					@Override
					public void updateLockState(String lockId, LockStatus lockStatus) {
						if (LockStatus.MASTER == lockStatus) {
							latch.countDown();
							mutexLock1.releaseLock(lockId, true);
						} else {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							mutexLock1.acquireLock(lockId, this);
						}

					}
				});

			}

		}).start();

		new Thread(new Runnable() {

			@Override
			public void run() {
				mutexLock2.acquireLock(lockResource, new LockUpdateCallback() {

					@Override
					public void updateLockState(String lockId, LockStatus lockStatus) {
						if (LockStatus.MASTER == lockStatus) {
							latch.countDown();
							mutexLock2.releaseLock(lockId, true);
						} else {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							mutexLock2.acquireLock(lockId, this);
						}
					}
				});

			}

		}).start();

		new Thread(new Runnable() {

			@Override
			public void run() {
				LockUpdateCallback callback = new LockUpdateCallback() {

					@Override
					public void updateLockState(String lockId, LockStatus lockStatus) {
						if (LockStatus.MASTER == lockStatus) {
							latch.countDown();
							mutexLock3.releaseLock(lockId, true);
						} else {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							mutexLock3.acquireLock(lockId, this);
						}
					}
				};
				mutexLock3.acquireLock(lockResource, callback);

			}

		}).start();

		latch.await();

	}

}
