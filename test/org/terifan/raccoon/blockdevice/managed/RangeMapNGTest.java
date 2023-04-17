package org.terifan.raccoon.blockdevice.managed;

import org.terifan.raccoon.blockdevice.managed.RangeMap;
import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class RangeMapNGTest
{
	@Test
	public void testSerialization1() throws IOException
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

		RangeMap inMap = new RangeMap();
		inMap.add(0, Integer.MAX_VALUE);
		inMap.remove(0, 10);
		inMap.marshal(buffer);

//		Log.hexDump(baos.toByteArray());

		RangeMap outMap = new RangeMap();
		outMap.unmarshal(ByteArrayBuffer.wrap(buffer.array()));

		assertEquals(inMap.toString(), outMap.toString());
	}


	@Test
	public void testSerialization2() throws IOException
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

		int limit = Integer.MAX_VALUE;

		RangeMap inMap = new RangeMap();
		inMap.add(0, limit);

		Random rnd = new Random(1);
		for (int i = 0; i >= 0 && i < limit; )
		{
			int j = 1 + Math.max(0, Math.min(rnd.nextInt(100000), limit - i));
			inMap.remove(i, j);
			i += j * 2;
		}

		inMap.marshal(buffer);

		RangeMap outMap = new RangeMap();
		outMap.unmarshal(ByteArrayBuffer.wrap(buffer.array()));

		assertEquals(inMap.toString(), outMap.toString());
	}
}
