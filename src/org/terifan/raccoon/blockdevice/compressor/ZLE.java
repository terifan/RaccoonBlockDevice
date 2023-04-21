package org.terifan.raccoon.blockdevice.compressor;

import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;


/**
 * This is a fast and simple algorithm to eliminate runs of zeroes.
 */
public class ZLE implements Compressor
{
	@Override
	public boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteBlockOutputStream aOutputStream)
	{
		try
		{
			for (int i = 0; i < aInputLength;)
			{
				int j = i;

				if (aInput[aInputOffset + i] == 0)
				{
					for (; j < aInputLength && aInput[aInputOffset + j] == 0; j++)
					{
					}
					ByteArrayBuffer.writeVar32(aOutputStream, j - i); // 1..n
				}
				else
				{
					for (; j < aInputLength - 1 && (aInput[aInputOffset + j] != 0 || aInput[aInputOffset + j + 1] != 0); j++)
					{
					}
					if (j == aInputLength - 1)
					{
						j++;
					}

					ByteArrayBuffer.writeVar32(aOutputStream, i - j + 1); // -n..0
					aOutputStream.write(aInput, aInputOffset + i, j - i);
				}

				i = j;
			}

			return aOutputStream.size() < aInputLength;
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);

			return false;
		}
	}

	// bug:
	// decompressing 4096 compressed bytes to 4200 bytes

	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException
	{
		ByteArrayBuffer input = ByteArrayBuffer.wrap(aInput).position(aInputOffset);
		ByteArrayBuffer output = ByteArrayBuffer.wrap(aOutput).position(aOutputOffset);

		for (int remaining = aOutputLength; remaining > 0;)
		{
			int len = input.readVar32();

			if (len > 0)
			{
				output.clear(len);
			}
			else
			{
				len = - (len - 1);
				input.transfer(output, len);
			}

			remaining -= len;
		}
	}
}
