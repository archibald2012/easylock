/**
 * 
 */
package org.easycluster.easylock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLock implements DistributedLock {

	private static final Logger	LOGGER			= LoggerFactory.getLogger(DefaultDistributedLock.class);

	private String				lockResource	= null;
	private LockManager			lockManager		= null;
	private volatile boolean	isAcquired		= false;
	private volatile String		lockId			= null;
	private volatile LockStatus	status			= LockStatus.STANDBY;
	private ReadWriteLock		statusLock		= new ReentrantReadWriteLock();

	public DefaultDistributedLock(final String lockResource) {
		this.lockResource = lockResource;
	}

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void awaitLock() throws InterruptedException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Acquiring a lock on lockResource " + lockResource);
		}

		if (isAcquired) {
			throw new LockException("Acquiring a duplicated lock on lockResource " + lockResource);
		}

		isAcquired = true;

		final CountDownLatch latch = new CountDownLatch(1);
		lockManager.acquireLock(lockResource, new LockUpdateCallback() {

			@Override
			public void updateLockState(String lockId, LockStatus lockStatus) {
				updateStatus(lockId, lockStatus);
				if (LockStatus.MASTER == lockStatus) {
					latch.countDown();
				}
			}
		});

		latch.await();

	}

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public boolean awaitLock(long timeout, TimeUnit unit) throws InterruptedException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Acquiring a lock on lockResource " + lockResource);
		}

		if (isAcquired) {
			throw new LockException("Acquiring a duplicated lock on lockResource " + lockResource);
		}

		isAcquired = true;

		final CountDownLatch latch = new CountDownLatch(1);
		lockManager.acquireLock(lockResource, new LockUpdateCallback() {

			@Override
			public void updateLockState(String lockId, LockStatus lockStatus) {
				updateStatus(lockId, lockStatus);
				if (LockStatus.MASTER == lockStatus) {
					latch.countDown();
				}
			}

		});

		return latch.await(timeout, unit);
	}

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void awaitLockUninterruptibly() {
		boolean completed = false;

		while (!completed) {
			try {
				awaitLock();
				completed = true;
			} catch (InterruptedException e) {
				unlock();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void lock(final LockUpdateCallback callback) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Acquiring a lock on lockResource " + lockResource);
		}
		if (isAcquired) {
			throw new LockException("Acquiring a duplicated lock on lockResource " + lockResource);
		}

		isAcquired = true;

		lockManager.acquireLock(lockResource, new LockUpdateCallback() {

			@Override
			public void updateLockState(String lockId, LockStatus lockStatus) {
				updateStatus(lockId, lockStatus);

				if (callback != null) {
					callback.updateLockState(lockId, lockStatus);
				}
			}
		});

	}

	private void updateStatus(String lockId, LockStatus lockStatus) {
		statusLock.writeLock().lock();
		try {
			this.lockId = lockId;
			this.status = lockStatus;
		} finally {
			statusLock.writeLock().unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unlock() {
		statusLock.writeLock().lock();
		try {
			if (lockId != null) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Releasing the lock " + lockId);
				}
				lockManager.releaseLock(lockId, false);
			}
			lockId = null;
			status = LockStatus.STANDBY;
			isAcquired = false;
		} finally {
			statusLock.writeLock().unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LockStatus getStatus() {
		statusLock.readLock().lock();
		try {
			return status;
		} finally {
			statusLock.readLock().unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getResource() {
		return lockResource;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setLockManager(LockManager lockManager) {
		this.lockManager = lockManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(super.toString());
		builder.append(",resource=").append(lockResource);
		builder.append(",status=").append(status);
		builder.append(",lockId=").append(lockId);
		return builder.toString();
	}

}
