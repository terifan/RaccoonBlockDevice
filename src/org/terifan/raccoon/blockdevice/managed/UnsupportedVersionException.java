package org.terifan.raccoon.blockdevice.managed;

import org.terifan.raccoon.blockdevice.RaccoonIOException;


public class UnsupportedVersionException extends RaccoonIOException
{
	private static final long serialVersionUID = 1L;


	public UnsupportedVersionException()
	{
	}


	public UnsupportedVersionException(String message)
	{
		super(message);
	}


	public UnsupportedVersionException(String message, Throwable cause)
	{
		super(message, cause);
	}


	public UnsupportedVersionException(Throwable cause)
	{
		super(cause);
	}
}
