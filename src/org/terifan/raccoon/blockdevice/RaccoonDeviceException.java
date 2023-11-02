package org.terifan.raccoon.blockdevice;


public class RaccoonDeviceException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public RaccoonDeviceException()
	{
	}


	public RaccoonDeviceException(String aMessage)
	{
		super(aMessage);
	}


	public RaccoonDeviceException(Throwable aCause)
	{
		super(aCause);
	}


	public RaccoonDeviceException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
