package org.terifan.raccoon.io.physical;

import org.terifan.raccoon.io.DeviceException;


public class FileAlreadyOpenException extends DeviceException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
