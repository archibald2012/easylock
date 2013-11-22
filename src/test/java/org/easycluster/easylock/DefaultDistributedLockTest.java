package org.easycluster.easylock;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.easycluster.easylock.zookeeper.ZooKeeperLockManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultDistributedLockTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testlock() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");
		lock.setLockManager(lockManager);

		int num = 10;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {

					try {
						lock.awaitLock(1000, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					latch.countDown();
				}
			}).start();
		}
		latch.await();
	}

	@Test
	public void testUnlock() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");
		lock.setLockManager(lockManager);

		int num = 10;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {

					try {
						lock.awaitLock(1000, TimeUnit.MILLISECONDS);
						lock.unlock();
						lock.unlock();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					latch.countDown();
				}
			}).start();
		}
		latch.await();
	}

	@Test
	public void testGetLockStatus() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");
		lock.setLockManager(lockManager);

		final CountDownLatch latch = new CountDownLatch(1);

		new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					Thread.sleep(500);
					System.out.println(lock.getStatus());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();

		new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					lock.awaitLock(1000, TimeUnit.MILLISECONDS);
					assertEquals(LockStatus.MASTER, lock.getStatus());
					lock.unlock();
					assertEquals(LockStatus.STANDBY, lock.getStatus());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				latch.countDown();
			}
		}).start();

		latch.await();

	}

	@Test
	public void testSimple() {
		final String lockResource = UUID.randomUUID().toString();
		final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");
		lock.setLockManager(lockManager);

		for (int i = 0; i < 100; i++) {
			try {
				lock.awaitLock();
				assertEquals(lockResource, lock.getResource());
				assertEquals(LockStatus.MASTER, lock.getStatus());

				lock.unlock();

				assertEquals(LockStatus.STANDBY, lock.getStatus());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	@Test
	public void testConcurrent() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");

		int num = 1000;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
					lock.setLockManager(lockManager);

					try {
						lock.awaitLock();
						assertEquals(lockResource, lock.getResource());
						assertEquals(LockStatus.MASTER, lock.getStatus());

						lock.unlock();

						assertEquals(LockStatus.STANDBY, lock.getStatus());

						latch.countDown();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}
			}).start();
		}
		latch.await();
	}

	@Test
	public void testLockWithCallback() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181");

		int num = 1000;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
					lock.setLockManager(lockManager);

					lock.lock(new LockUpdateCallback() {

						@Override
						public void updateLockState(String lockId, LockStatus lockStatus) {
							assertEquals(lockResource, lock.getResource());
							if (LockStatus.MASTER == lock.getStatus()) {
								assertEquals(LockStatus.MASTER, lock.getStatus());
								lock.unlock();
								assertEquals(LockStatus.STANDBY, lock.getStatus());
								latch.countDown();
							}
						}
					});

				}
			}).start();
		}
		latch.await();
	}

}
