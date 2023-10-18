package org.terifan.raccoon.blockdevice.managed;


/**
 * BlockDevice forced synchronization hint.
 */
public enum SyncMode
{
	/**
	 * Disable forced synchronization. (Reference time: 5s)
	 */
	OFF,
	/**
	 * Synchronizes changes a single time when the commit method on a database is called. Calls the {@link java.nio.channels.FileChannel#force} method on a commit after data is written. (Reference time: 35s)
	 */
	SINGLE,
	/**
	 * Synchronizes changes twice when the commit method on a database is called. Calls the {@link java.nio.channels.FileChannel#force} method, first to ensure all data is written and then to ensure the SuperBlock is written. (Reference time: 43s)
	 */
	DOUBLE,
	/**
	 * Synchronizes changes a single time when the device is closing. Calls the {@link java.nio.channels.FileChannel#force} method on close. (Reference time: 35s)
	 */
	ONCLOSE
}
