package org.terifan.raccoon.blockdevice;


public class RaccoonIOException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public RaccoonIOException()
	{
	}


	public RaccoonIOException(String aMessage)
	{
		super(aMessage);
	}


	public RaccoonIOException(Throwable aCause)
	{
		super(aCause);
	}


	public RaccoonIOException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
