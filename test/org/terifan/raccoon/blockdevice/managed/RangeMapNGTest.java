package org.terifan.raccoon.blockdevice.managed;

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
		inMap.add(0, Long.MAX_VALUE);
		inMap.remove(0, 10);
		inMap.marshal(buffer);

		RangeMap outMap = new RangeMap();
		outMap.unmarshal(ByteArrayBuffer.wrap(buffer.array()));

		assertEquals(inMap.toString(), outMap.toString());
	}


	@Test
	public void testSerialization2() throws IOException
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(16);

		long limit = Long.MAX_VALUE;

		RangeMap inMap = new RangeMap();
		inMap.add(0, limit);

		Random rnd = new Random(1);
		for (int i = 0; i >= 0 && i < limit;)
		{
			long j = 1 + Math.max(0, Math.min(rnd.nextInt(100000), limit - i));
			inMap.remove(i, j);
			i += j * 2;
		}

		inMap.marshal(buffer);

		RangeMap outMap = new RangeMap();
		outMap.unmarshal(ByteArrayBuffer.wrap(buffer.array()));

		assertEquals(inMap.toString(), outMap.toString());
	}


	@Test
	public void testAddRemove() throws IOException
	{
		long limit = Long.MAX_VALUE;

		RangeMap map = new RangeMap();
		map.add(0, limit);
		assertEquals(map.toString(), "{0-9223372036854775806}");
		map.next(16);
		assertEquals(map.toString(), "{16-9223372036854775806}");
		map.add(4, 8);
		assertEquals(map.toString(), "{4-11, 16-9223372036854775806}");
		map.remove(6, 4);
		assertEquals(map.toString(), "{4-5, 10-11, 16-9223372036854775806}");
	}


	@Test
	public void testClone() throws IOException
	{
		long limit = Long.MAX_VALUE;

		RangeMap inMap = new RangeMap();
		inMap.add(0, limit);
		inMap.next(16);
		inMap.add(4, 8);
		inMap.remove(6, 4);

		RangeMap outMap = inMap.clone();

		assertEquals(inMap.toString(), outMap.toString());
	}
}
