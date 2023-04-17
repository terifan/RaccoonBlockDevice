package org.terifan.raccoon.blockdevice;


public class EOFException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public EOFException(String aMessage)
	{
		super(aMessage);
	}
}
