package org.terifan.raccoon.blockdevice;


public enum BlockDeviceOpenOption
{
	/**
	 * Open an already existing device for both read and write mode causing an exception if not found.
	 */
	OPEN,
	/**
	 * Open or create a new device for both read and write mode.
	 */
	CREATE,
	/**
	 * Replace any existing device and create a new for both read and write mode.
	 */
	REPLACE,
	/**
	 * Open an existing device for only read mode.
	 */
	READ_ONLY
}
