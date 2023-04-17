package org.terifan.raccoon.blockdevice.util;

import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.blockdevice.EOFException;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class ByteArrayBufferNGTest
{
	@Test
	public void testReadAndWriteVariableBuffer() throws IOException
	{
		Random rnd = new Random(1);
		byte[] buf = new byte[100];
		rnd.nextBytes(buf);

		rnd = new Random(1);

		ByteArrayBuffer out = ByteArrayBuffer.alloc(50);
		for (int j = 0; j < 10; j++)
		{
			for (int i = rnd.nextInt(100); --i >= 0; )
			{
				out.writeInt8(rnd.nextInt() & 0xff);
			}
			out.writeInt32(rnd.nextInt());
			out.writeInt32(Integer.MIN_VALUE);
			out.writeInt32(Integer.MAX_VALUE);
			out.writeInt64(rnd.nextLong());
			out.writeInt64(Long.MIN_VALUE);
			out.writeInt64(Long.MAX_VALUE);
			out.writeVar32(rnd.nextInt());
			out.writeVar32(Integer.MIN_VALUE);
			out.writeVar32(Integer.MAX_VALUE);
			out.writeVar64(rnd.nextLong());
			out.writeVar64(Long.MIN_VALUE);
			out.writeVar64(Long.MAX_VALUE);
			out.writeFloat(rnd.nextFloat());
			out.writeDouble(rnd.nextDouble());
			out.writeString("hello world");
			out.write(buf);
		}

		assertEquals(out.position(), 2607);
//		assertEquals(out.capacity(), 2779);
//		assertEquals(out.remaining(), 2779 - 2607);

		ByteArrayBuffer in = ByteArrayBuffer.wrap(out.trim().array());
		rnd = new Random(1);

		for (int j = 0; j < 10; j++)
		{
			for (int i = rnd.nextInt(100); --i >= 0; )
			{
				assertEquals(in.readInt8(), rnd.nextInt() & 0xff);
			}
			assertEquals(in.readInt32(), rnd.nextInt());
			assertEquals(in.readInt32(), Integer.MIN_VALUE);
			assertEquals(in.readInt32(), Integer.MAX_VALUE);
			assertEquals(in.readInt64(), rnd.nextLong());
			assertEquals(in.readInt64(), Long.MIN_VALUE);
			assertEquals(in.readInt64(), Long.MAX_VALUE);
			assertEquals(in.readVar32(), rnd.nextInt());
			assertEquals(in.readVar32(), Integer.MIN_VALUE);
			assertEquals(in.readVar32(), Integer.MAX_VALUE);
			assertEquals(in.readVar64(), rnd.nextLong());
			assertEquals(in.readVar64(), Long.MIN_VALUE);
			assertEquals(in.readVar64(), Long.MAX_VALUE);
			assertEquals(in.readFloat(), rnd.nextFloat());
			assertEquals(in.readDouble(), rnd.nextDouble());
			assertEquals(in.readString(11), "hello world");
			byte[] readBuf = in.read(new byte[100]);
			assertEquals(readBuf, buf);
		}
	}


	@Test(expectedExceptions = EOFException.class)
	public void testWriteOverflow1() throws IOException
	{
		ByteArrayBuffer out = ByteArrayBuffer.wrap(new byte[5]);
		out.writeInt64(0);
	}


	@Test(expectedExceptions = EOFException.class)
	public void testWriteOverflow2() throws IOException
	{
		ByteArrayBuffer out = ByteArrayBuffer.wrap(new byte[5]);
		out.writeInt8(1);
		out.writeInt8(2);
		out.writeInt8(3);
		out.writeInt8(4);
		out.writeInt8(5);
		out.writeInt8(6);
	}


	@Test
	public void testBitBuffering1() throws IOException
	{
		ByteArrayBuffer out = ByteArrayBuffer.alloc(10);
		out.writeBit(1);
		out.writeBits(0b1010101, 7);
		out.writeBits(0b1001, 4);
		out.writeBits(0b111101, 6);

		byte[] buf = out.array();

		assertEquals(0xff & buf[0], 0b11010101);
		assertEquals(0xff & buf[1], 0b10011111);
		assertEquals(0xff & buf[2], 0b01000000);

		ByteArrayBuffer in = ByteArrayBuffer.wrap(buf);
		assertEquals(in.readBit(), 1);
		assertEquals(in.readBits(7), 0b1010101);
		assertEquals(in.readBits(4), 0b1001);
		assertEquals(in.readBits(6), 0b111101);
	}


	@Test
	public void testBitBuffering2() throws IOException
	{
		ByteArrayBuffer out = ByteArrayBuffer.alloc(4);
		out.writeBit(1);
		out.writeBit(0);
		out.writeBit(1);
		out.writeBit(1);
		out.align();
		out.writeInt8(0b10101010);

		byte[] buf = out.array();

		assertEquals(buf, new byte[]{(byte)0b10110000, (byte)0b10101010, 0,0});

		ByteArrayBuffer in = ByteArrayBuffer.wrap(buf);
		assertEquals(in.readBits(4), 0b1011);
		out.align();
		assertEquals(in.readInt8(), 0b10101010);
	}


	@Test
	public void testBitBuffering3() throws IOException
	{
		ByteArrayBuffer out = ByteArrayBuffer.alloc(4);
		out.writeBit(1);
		out.align();
		out.writeInt8(0b10101010);
		out.position(1);
		out.writeBit(1);
		out.align();
		out.writeInt8(0b10101010);

		out.position(0);
		assertEquals(out.readBit(), 1);
		out.align();
		out.position(1);
		assertEquals(out.readBit(), 1);
		out.align();
		assertEquals(out.readInt8(), 0b10101010);
	}
}
