package org.terifan.raccoon.blockdevice;

import java.io.OutputStream;
import java.util.Arrays;


/**
 * OutputStream will grow buffer in steps of a defined size
 */
public class ByteBlockOutputStream extends OutputStream
{
	private int mOffset;
	private byte[] mBuffer;
	private int mBlockSize;


	/**
	 *
	 * @param aBlockSize
	 *   initial size as well as the size the internal buffer will grow with when becoming full
	 */
	public ByteBlockOutputStream(int aBlockSize)
	{
		mBuffer = new byte[aBlockSize];
		mBlockSize = aBlockSize;
	}


	@Override
	public void write(int aByte)
	{
		if (mOffset == mBuffer.length)
		{
			mBuffer = Arrays.copyOfRange(mBuffer, 0, mOffset + mBlockSize);
		}

		mBuffer[mOffset++] = (byte)aByte;
	}


	@Override
	public void write(byte[] aBuffer, int aOffset, int aLength)
	{
		while (aLength > 0)
		{
			if (mOffset == mBuffer.length)
			{
				mBuffer = Arrays.copyOfRange(mBuffer, 0, mOffset + mBlockSize);
			}

			int s = Math.min(aLength, mBuffer.length - mOffset);

			System.arraycopy(aBuffer, aOffset, mBuffer, mOffset, s);

			mOffset += s;
			aOffset += s;
			aLength -= s;
		}
	}


	public byte[] getBuffer()
	{
		return mBuffer;
	}


	public int size()
	{
		return mOffset;
	}
}
