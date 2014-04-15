package org.easycluster.easylock.lifecycle;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.easycluster.easylock.DistributedLock;
import org.easycluster.easylock.LockStatus;
import org.easycluster.easylock.LockUpdateCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLifecycle implements CoreLifecycle,
		LockUpdateCallback {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AbstractLifecycle.class);

	private LifecycleState state = LifecycleState.INITIAL;

	private ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

	private DistributedLock distributedLock;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isAllowTo(LifecycleState newState) {
		stateLock.readLock().lock();
		try {
			switch (newState) {
			case INITIAL:
				if (state == LifecycleState.PREPARED) {
					return true;
				}
				break;
			case PREPARED:
				if (state == LifecycleState.INITIAL
						|| state == LifecycleState.ACTIVATED) {
					return true;
				}
				break;
			case ACTIVATED:
				if (state == LifecycleState.PREPARED) {
					return true;
				}
				break;
			}

			// now there is a problem
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Cannot change to state " + newState
						+ " from state " + state);
			}
		} finally {
			stateLock.readLock().unlock();
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LifecycleState updateState(LifecycleState newState, long waitTime) {
		if (newState == LifecycleState.INITIAL) {
			// need to downgrade
			if (getState() == LifecycleState.ACTIVATED) {
				suspend();
				waitState(this, LifecycleState.PREPARED, waitTime);
			}
			if (getState() == LifecycleState.PREPARED) {
				release();
			}
		} else if (newState == LifecycleState.PREPARED) {
			if (getState() == LifecycleState.ACTIVATED) {
				// need to downgrade
				suspend();
				waitState(this, LifecycleState.PREPARED, waitTime);
			}
			if (getState() == LifecycleState.INITIAL) {
				// need to upgrade
				prepare();
			}
		} else if (newState == LifecycleState.ACTIVATED) {
			// need to upgrade
			if (getState() == LifecycleState.INITIAL) {
				prepare();
			}
			if (getState() == LifecycleState.PREPARED) {
				activate();
				waitState(this, LifecycleState.ACTIVATED, waitTime);
			}
		}

		return getState();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setState(LifecycleState state) {
		LifecycleState oldState = null;

		// Assume the caller has check with the isAllowTo method
		// for implementing state transition control
		stateLock.writeLock().lock();
		try {
			oldState = this.state;
			this.state = state;
		} finally {
			stateLock.writeLock().unlock();
		}

		// lifecycle is updated
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Changed state from " + oldState + " to " + state
					+ " in component " + this);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LifecycleState getState() {
		stateLock.readLock().lock();
		try {
			return state;
		} finally {
			stateLock.readLock().unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public final void prepare() {
		if (isAllowTo(LifecycleState.PREPARED)) {
			doPrepare();

			setState(LifecycleState.PREPARED);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public final void activate() {
		if (isAllowTo(LifecycleState.ACTIVATED)) {
			if (distributedLock != null) {
				distributedLock.lock(this);
			} else {
				doActivate();

				setState(LifecycleState.ACTIVATED);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public final void suspend() {
		if (isAllowTo(LifecycleState.PREPARED)) {
			if (distributedLock != null) {
				distributedLock.unlock();
			} else {
				doSuspend();

				setState(LifecycleState.PREPARED);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public final void release() {
		if (isAllowTo(LifecycleState.INITIAL)) {
			doRelease();

			setState(LifecycleState.INITIAL);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateLockState(String lockId, LockStatus lockStatus) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Lock update callback on lockResource "
					+ distributedLock.getResource() + " lockStatus "
					+ lockStatus);
		}

		LifecycleState state = getState();
		if (lockStatus == LockStatus.MASTER) {
			boolean isActivated = true;
			if (state != LifecycleState.ACTIVATED) {
				if (isAllowTo(LifecycleState.ACTIVATED)) {
					isActivated = doActivate();
					if (isActivated) {
						setState(LifecycleState.ACTIVATED);
					} else {
						// failed to activate the component
						if (LOGGER.isWarnEnabled()) {
							LOGGER.warn("Failed to activate component " + this);
						}
					}
				} else {
					if (LOGGER.isWarnEnabled()) {
						LOGGER.warn("Unable to activate the component " + this
								+ " with state " + state + " with lockStatue "
								+ lockStatus);
					}
					isActivated = false;
				}
			} else {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("The component " + this
							+ " is already activated");
				}
			}
			// so we need to release the lock
			if (!isActivated && distributedLock != null) {
				distributedLock.unlock();
			}
		} else if (lockStatus == LockStatus.STANDBY) {
			// Now change the lifecycle to suspended
			boolean isSuspended = true;
			if (state == LifecycleState.ACTIVATED) {
				if (isAllowTo(LifecycleState.PREPARED)) {
					isSuspended = doSuspend();
					if (isSuspended) {
						setState(LifecycleState.PREPARED);
					} else {
						// failed to suspend the component
						if (LOGGER.isWarnEnabled()) {
							LOGGER.warn("Failed to suspend component " + this);
						}
					}
				} else {
					// the component cannot be suspended
					if (LOGGER.isWarnEnabled()) {
						LOGGER.warn("Unable to suspend the component " + this
								+ " with state " + state + " with lockStatue "
								+ lockStatus);
					}
					isSuspended = false;
				}

				// allow it to release the lock
				if (isSuspended && distributedLock != null) {
					// as long it is suspended, we need to release the lock
					distributedLock.unlock();
				}
			}

		} else {
			// unknown lock status
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("The component " + this
						+ " received unknown lock status " + lockStatus);
			}
		}
	}

	LifecycleState waitState(CoreLifecycle lifecycle,
			LifecycleState targetState, long waitTime) {
		long targetTime = System.currentTimeMillis() + waitTime;
		LifecycleState currentState = lifecycle.getState();
		while (currentState != targetState) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException ignored) {
			}

			currentState = lifecycle.getState();
			if (System.currentTimeMillis() > targetTime) {
				break;
			}
		}

		return currentState;
	}

	/**
	 * Prepare the component now.
	 * 
	 * @return
	 */
	protected boolean doPrepare() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Component " + this + " is preparing now");
		}
		return true;
	}

	/**
	 * Activate the component now.
	 * 
	 * @return
	 */
	protected boolean doActivate() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Component " + this + " is activating now");
		}
		return true;
	}

	/**
	 * Suspend the component now.
	 * 
	 * @return
	 */
	protected boolean doSuspend() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Component " + this + " is suspending now");
		}
		return true;
	}

	/**
	 * Disengage the component now.
	 * 
	 * @return
	 */
	protected boolean doRelease() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Component " + this + " is releasing now");
		}
		return true;
	}

	public void setDistributedLock(DistributedLock distributedLock) {
		this.distributedLock = distributedLock;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(super.toString());

		builder.append(",state=").append(getState());

		return builder.toString();
	}
}
