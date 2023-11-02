package org.terifan.raccoon.blockdevice.storage;

import org.terifan.raccoon.blockdevice.RaccoonDeviceException;


public class FileAlreadyOpenException extends RaccoonDeviceException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
