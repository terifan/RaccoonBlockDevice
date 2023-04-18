package org.terifan.raccoon.blockdevice;


public class RaccoonIOException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public RaccoonIOException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
