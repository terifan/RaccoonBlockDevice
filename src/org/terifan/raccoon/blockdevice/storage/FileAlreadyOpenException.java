package org.terifan.raccoon.blockdevice.storage;

import org.terifan.raccoon.blockdevice.RaccoonIOException;


public class FileAlreadyOpenException extends RaccoonIOException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
