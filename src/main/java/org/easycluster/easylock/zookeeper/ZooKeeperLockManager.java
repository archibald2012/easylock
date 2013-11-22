package org.easycluster.easylock.zookeeper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.easycluster.easylock.LockException;
import org.easycluster.easylock.LockManager;
import org.easycluster.easylock.LockStatus;
import org.easycluster.easylock.LockUpdateCallback;
import org.easycluster.easylock.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperLockManager implements LockManager {

	private static final Logger					LOGGER			= LoggerFactory.getLogger(ZooKeeperLockManager.class);

	private static final String					ZK_LOCK_ROOT	= "/Locks";

	private String								lockInstance	= SystemUtil.getHostName() + ":" + SystemUtil.getPid();
	private String								lockRootNode	= null;
	private String								connectString	= null;
	private int									sessionTimeout	= 0;
	private volatile ZooKeeper					zooKeeper		= null;
	private volatile LockWatcher				watcher			= null;
	private volatile CountDownLatch				connectedLatch	= null;
	private ScheduledExecutorService			connExec		= Executors.newSingleThreadScheduledExecutor();

	private ConcurrentHashMap<String, LockData>	lockDataStore	= new ConcurrentHashMap<String, LockData>();

	public ZooKeeperLockManager(final String zooKeeperConnectString) {
		this(zooKeeperConnectString, 30000, ZK_LOCK_ROOT);
	}

	public ZooKeeperLockManager(final String zooKeeperConnectString, final int zooKeeperSessionTimeoutMillis) {
		this(zooKeeperConnectString, zooKeeperSessionTimeoutMillis, ZK_LOCK_ROOT);
	}

	public ZooKeeperLockManager(final String zooKeeperConnectString, final int zooKeeperSessionTimeoutMillis, final String lockRootNode) {
		this.connectString = zooKeeperConnectString;
		this.sessionTimeout = zooKeeperSessionTimeoutMillis;
		this.lockRootNode = lockRootNode;

		connectZooKeeper();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void acquireLock(String lockResource, LockUpdateCallback callback) {
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(lockInstance + " is acquiring a lock on lockResource " + lockResource);
		}

		try {
			verifyZooKeeperStructure(zooKeeper, lockRootNode + "/" + lockResource);
			String lockId = createZNode(zooKeeper, lockRootNode + "/" + lockResource + "/" + lockInstance + "-", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
			lockDataStore.put(lockId, new LockData(lockResource, callback));
			checkLockStatus(lockId, lockResource);
		} catch (Exception ex) {
			throw new LockException("Unhandled exception while working with ZooKeeper", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseLock(String lockId, boolean notify) {
		if (lockId == null) {
			throw new IllegalArgumentException("lockId is null");
		}
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(lockInstance + " is releasing the lock " + lockId);
		}
		try {
			zooKeeper.delete(lockId, -1);
		} catch (NoNodeException noNodeException) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ZNode [" + lockId + "] not exists.");
			}
			// do nothing
		} catch (KeeperException e) {
			throw new LockException("Unhandled exception while working with ZooKeeper", e);
		} catch (InterruptedException e) {
			throw new LockException("Unhandled exception while working with ZooKeeper", e);
		}

		LockData data = lockDataStore.remove(lockId);
		if (notify && data != null) {
			LockUpdateCallback updateCallback = data.getUpdateCallback();
			if (updateCallback != null) {
				updateCallback.updateLockState(lockId, LockStatus.STANDBY);
			}

		}
	}

	public String getLockInstance() {
		return lockInstance;
	}

	public boolean isConnected() {
		return connectedLatch.getCount() == 0;
	}

	public void setLockInstance(String lockInstance) {
		this.lockInstance = lockInstance;
	}

	private void checkLockStatus(final String lockId, final String lockResource) throws KeeperException, InterruptedException {

		String lockPath = lockRootNode + "/" + lockResource;

		// get all children of lock znode and find the one that is just
		// before us, if
		// any. This must be inside loop, as children might get deleted
		// out of order because
		// of client disconnects. We cannot assume that the file that is
		// in front of us this
		// time, is there next time. It might have been deleted even
		// though earlier files
		// are still there.
		List<String> children = zooKeeper.getChildren(lockPath, false);
		if (children.isEmpty()) {
			String error = "No children in [" + lockPath + "] although one was just created. just failed lock progress.";
			LOGGER.error(error);
			//throw new LockException(error);
			return;
		}

		// check what is our ID (sequence number at the end of file name)
		int mySeqNum = Integer.parseInt(lockId.substring(lockId.lastIndexOf('-') + 1));
		int previousSeqNum = -1;
		String predecessor = null;

		for (String child : children) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("child: " + child);
			}

			int otherSeqNum = Integer.parseInt(child.substring(child.lastIndexOf('-') + 1));
			if ((otherSeqNum < mySeqNum) && (otherSeqNum > previousSeqNum)) {
				previousSeqNum = otherSeqNum;
				predecessor = child;
			}
		}

		// our sequence number is smallest, we have the lock
		if (-1 == previousSeqNum) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("No smaller znode sequences, " + lockId + " acquired lock");
			}
			updateStatus(lockId, LockStatus.MASTER);
			return;
		}

		Watcher watcher = new Watcher() {

			public void process(WatchedEvent event) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Received watched event {}", event);
				}
				try {
					// The watch will be triggered by a successful operation
					// that creates/delete the node or sets the data on
					// the node.
					checkLockStatus(lockId, lockResource);
				} catch (Exception e) {
					throw new LockException(e.getMessage(), e);
				}
			}
		};

		// The removal of a node will only cause one client to wake up to
		// avoid herd affect.
		if (zooKeeper.exists(lockPath + "/" + predecessor, watcher) == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(predecessor + " does not exists, " + lockId + " acquired lock");
			}
			updateStatus(lockId, LockStatus.MASTER);
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(predecessor + " is still here, " + lockId + " must blocked for wait");
			}
			updateStatus(lockId, LockStatus.STANDBY);
		}

	}

	private String createZNode(ZooKeeper zk, String znode, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
		String nodePath = null;
		try {
			nodePath = zk.create(znode, data, Ids.OPEN_ACL_UNSAFE, mode);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ZNode created, [" + nodePath + "]");
			}
		} catch (NodeExistsException existException) {
			nodePath = existException.getPath();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ZNode [" + nodePath + "] exists, just skip.");
			}
		} catch (NoNodeException noNodeException) {
			nodePath = noNodeException.getPath();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ZNode [" + nodePath + "] not exists.");
			}
			throw noNodeException;
		}
		return nodePath;
	}

	private void updateStatus(String lockId, LockStatus newStatus) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("updateStatus - lockInstance=[{}], lockId=[{}], status=[{}]", new Object[] { lockInstance, lockId, newStatus });
		}
		LockData data = lockDataStore.get(lockId);
		if (data != null) {
			LockUpdateCallback updateCallback = data.getUpdateCallback();
			if (updateCallback != null) {
				updateCallback.updateLockState(lockId, newStatus);
			}
		} else {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("No lock update data found. lockId=[{}]", lockId);
			}
		}
	}

	private void connectZooKeeper() {
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("Connecting to ZooKeeper {} ...", connectString);
		}

		connectedLatch = new CountDownLatch(1);

		try {
			watcher = new LockWatcher(this);
			zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);

			boolean connected = connectedLatch.await(10, TimeUnit.SECONDS);
			if (connected) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("Connected to ZooKeeper");
				}
			} else {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("Connect to ZooKeeper timeout with 10 seconds.");
				}
				connExec.schedule(new Runnable() {
					@Override
					public void run() {
						connectZooKeeper();
					}

				}, 10, TimeUnit.SECONDS);
			}

		} catch (Exception e) {
			String error = "Exception while connecting to ZooKeeper";
			LOGGER.error(error, e);
			throw new LockException(error, e);
		}
	}

	void handleConnected() {
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("handleConnected");
		}

		verifyZooKeeperStructure(zooKeeper, lockRootNode);
		connectedLatch.countDown();
	}

	void handleDisconnected() {
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("handleDisconnected");
		}
		connectedLatch = new CountDownLatch(1);
	}

	void handleExpired() {
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("handleExpired");
		}

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("Connection to ZooKeeper expired, reconnecting...");
		}
		try {
			for (String lockId : lockDataStore.keySet()) {
				updateStatus(lockId, LockStatus.STANDBY);
			}
			lockDataStore.clear();
			watcher.shutdown();
			connectZooKeeper();
		} catch (Exception ex) {
			LOGGER.error("Unhandled exception while working with ZooKeeper", ex);
		}
	}

	private void verifyZooKeeperStructure(ZooKeeper zk, String path) {
		try {
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("Ensuring {} exists", path);
			}
			if (zk.exists(path, false) == null) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("{} doesn't exist, creating", path);
				}
				zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (NodeExistsException ex) {
			// do nothing
		} catch (KeeperException e) {
			LOGGER.error("Unhandled exception while working with ZooKeeper", e);
		} catch (InterruptedException e) {
			LOGGER.error("Unhandled exception while working with ZooKeeper", e);
		}

	}

	class LockWatcher implements Watcher {
		private volatile boolean		shutdownSwitch	= false;

		private ZooKeeperLockManager	zooKeeperLockManager;

		public LockWatcher(ZooKeeperLockManager zooKeeperLockManager) {
			this.zooKeeperLockManager = zooKeeperLockManager;
		}

		public void process(WatchedEvent event) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Received watched event {}", event);
			}

			if (shutdownSwitch) {
				return;
			}

			if (event.getType() == EventType.None) {
				if (event.getState() == KeeperState.SyncConnected) {
					zooKeeperLockManager.handleConnected();
				} else if (event.getState() == KeeperState.Expired) {
					zooKeeperLockManager.handleExpired();
				} else if (event.getState() == KeeperState.Disconnected) {
					zooKeeperLockManager.handleDisconnected();
				}
			}
		}

		public void shutdown() {
			shutdownSwitch = true;
		}
	}

	class LockData {
		private String				lockResource;
		private LockUpdateCallback	updateCallback;

		public LockData(String lockResource, LockUpdateCallback updateCallback) {
			this.lockResource = lockResource;
			this.updateCallback = updateCallback;
		}

		public String getLockResource() {
			return lockResource;
		}

		public LockUpdateCallback getUpdateCallback() {
			return updateCallback;
		}

	}
}
