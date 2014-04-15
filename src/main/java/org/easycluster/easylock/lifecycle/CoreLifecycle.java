package org.easycluster.easylock.lifecycle;

public interface CoreLifecycle {

	/**
	 * 初始化
	 */
	void prepare();

	/**
	 * 激活
	 */
	void activate();

	/**
	 * 暂停
	 */
	void suspend();

	/**
	 * 释放
	 */
	void release();

	/**
	 * 是否允许切换到新的状态
	 * 
	 * @param newState
	 * @return
	 */
	boolean isAllowTo(LifecycleState newState);

	/**
	 * 更新组件至新的状态，该方法内可能会涉及到状态的多次切换
	 * 
	 * @param newState
	 * @param waitTime
	 * @return
	 */
	LifecycleState updateState(LifecycleState newState, long waitTime);

	/**
	 * 设置当前状态.
	 * 
	 * @param state
	 */
	void setState(LifecycleState state);

	/**
	 * 当前状态
	 * 
	 * @return
	 */
	LifecycleState getState();
}
