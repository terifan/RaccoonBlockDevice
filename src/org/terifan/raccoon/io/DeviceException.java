package org.terifan.raccoon.io;


public class DeviceException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public DeviceException()
	{
	}


	public DeviceException(String aMessage)
	{
		super(aMessage);
	}


	public DeviceException(Throwable aCause)
	{
		super(aCause);
	}


	public DeviceException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
