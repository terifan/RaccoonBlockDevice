package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.io.DeviceException;


public class UnsupportedVersionException extends DeviceException
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
