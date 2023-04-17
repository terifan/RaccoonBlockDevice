package org.terifan.raccoon.blockdevice.physical;

import org.terifan.raccoon.blockdevice.DeviceException;


public class FileAlreadyOpenException extends DeviceException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
