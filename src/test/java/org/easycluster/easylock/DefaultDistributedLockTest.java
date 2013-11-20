package org.easycluster.easylock;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181", 30000);
		lock.setLockManager(lockManager);

		int num = 10;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {

					lock.lock();
					lock.lock();

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
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181", 30000);
		lock.setLockManager(lockManager);

		int num = 10;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {

					try {
						Future<String> future = lock.lock();
						String lockId = future.get(1000, TimeUnit.MILLISECONDS);
						Thread.sleep(1000);
						lock.unlock(lockId);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					} catch (TimeoutException e) {
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
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181", 30000);
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
					Future<String> future = lock.lock();
					String lockId = future.get(1000, TimeUnit.MILLISECONDS);
					assertEquals(LockStatus.MASTER, lock.getStatus());
					Thread.sleep(1000);
					lock.unlock(lockId);
					assertEquals(LockStatus.STANDBY, lock.getStatus());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
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
		ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181", 30000);
		lock.setLockManager(lockManager);

		for (int i = 0; i < 100; i++) {
			try {
				Future<String> future = lock.lock();
				String lockId = future.get(1000, TimeUnit.MILLISECONDS);
				assertEquals(lockResource, lock.getLockResource());
				assertEquals(LockStatus.MASTER, lock.getStatus());

				lock.unlock(lockId);

				assertEquals(LockStatus.STANDBY, lock.getStatus());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}

		}
	}

	@Test
	public void testConcurrent() throws Exception {
		final String lockResource = UUID.randomUUID().toString();
		final ZooKeeperLockManager lockManager = new ZooKeeperLockManager("127.0.0.1:2181", 30000);

		int num = 1000;
		final CountDownLatch latch = new CountDownLatch(num);
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					final DefaultDistributedLock lock = new DefaultDistributedLock(lockResource);
					lock.setLockManager(lockManager);

					Future<String> future = lock.lock();
					try {
						String lockId = future.get();
						assertEquals(lockResource, lock.getLockResource());
						assertEquals(LockStatus.MASTER, lock.getStatus());

						lock.unlock(lockId);

						assertEquals(LockStatus.STANDBY, lock.getStatus());

						latch.countDown();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

				}
			}).start();
		}
		latch.await();
	}

}
