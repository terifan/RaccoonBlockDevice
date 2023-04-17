package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.security.random.SecureRandom;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
	@Test
	public void testSomeMethod()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.HOLE)
			.setBlockLevel(0x02)
			.setChecksumAlgorithm((byte)0x03)
			.setCompressionAlgorithm((byte)0x04)
			.setAllocatedSize(0x05060708)
			.setLogicalSize(0x09101112)
			.setPhysicalSize(0x13141516)
			.setBlockIndex0(0x1718192021222324L)
			.setBlockIndex1(0x2526272829303132L)
			.setBlockIndex2(0x3334353637383940L)
			.setTransactionId(0x4950515253545556L)
			.setBlockKey(new SecureRandom(1).ints(8).toArray())
			.setChecksum(new long[]{0x8990919293949596L,0x9798990102030405L,0x0607080910111213L,0x1415161718192021L})
			;

		Log.hexDump(bp.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), 8);
	}
}
