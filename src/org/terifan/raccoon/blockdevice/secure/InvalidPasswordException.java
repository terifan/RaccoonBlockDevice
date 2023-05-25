package org.terifan.raccoon.blockdevice.secure;


public class InvalidPasswordException extends RuntimeException
{
	private final static long serialVersionUID = 1L;


	public InvalidPasswordException()
	{
	}


	public InvalidPasswordException(String aMessage)
	{
		super(aMessage);
	}
}
