/**
 * 
 */
package org.easycluster.easylock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLock implements DistributedLock {

	private static final Logger	LOGGER			= LoggerFactory.getLogger(DefaultDistributedLock.class);

	private String				lockResource	= null;
	protected LockManager		lockManager		= null;
	private volatile LockStatus	status			= LockStatus.STANDBY;
	private ReentrantLock		statusLock		= new ReentrantLock();

	public DefaultDistributedLock(final String lockResource) {
		this.lockResource = lockResource;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<String> lock() {

		statusLock.lock();

		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Acquiring a lock on lockResource " + lockResource);
			}
			final ResponseFuture future = new ResponseFuture();

			lockManager.acquireLock(lockResource, new LockUpdateCallback() {

				@Override
				public void updateLockState(String lockId, LockStatus lockStatus, Object updateData) {
					status = lockStatus;
					if (LockStatus.MASTER == lockStatus) {
						future.offerResponse(lockId);
					}
				}
			}, null);

			return future;
		} finally {
			statusLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unlock(String lockId) {
		if (lockId == null) {
			throw new IllegalArgumentException("lockId is null");
		}

		statusLock.lock();
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Releasing the lock " + lockId);
			}
			lockManager.releaseLock(lockId, true);
		} finally {
			statusLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LockStatus getStatus() {
		return status;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getLockResource() {
		return lockResource;
	}

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
		return builder.toString();
	}

}

class ResponseFuture implements Future<String> {

	private CountDownLatch	latch		= new CountDownLatch(1);
	private volatile String	response	= null;

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return latch.getCount() == 0;
	}

	@Override
	public String get() throws InterruptedException, ExecutionException {
		latch.await();
		return response;
	}

	@Override
	public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		boolean result = latch.await(timeout, unit);
		if (!result || response == null) {
			throw new TimeoutException("Timed out waiting for response");
		}

		return response;
	}

	public void offerResponse(String resp) {
		response = resp;
		latch.countDown();
	}

}
