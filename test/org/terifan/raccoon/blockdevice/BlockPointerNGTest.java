package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
	@Test
	public void testSomeMethod()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.FREE)
			.setBlockLevel(1)
			.setChecksumAlgorithm(2)
			.setCompressionAlgorithm(3)
			.setAllocatedSize(32768)
			.setLogicalSize(31264)
			.setPhysicalSize(21679)
			.setAddress(Array.of(26161))
			.setGeneration(71)
			.setBlockKey(Array.of(0x88888888,0x99999999,0xaaaaaaaa,0xbbbbbbbb))
			.setChecksum(Array.of(0xcccccccc,0xcccccccc,0xdddddddd,0xdddddddd))
			;

		System.out.println(bp.toByteArray().length);

		System.out.println(bp);

		Log.hexDump(bp.toByteArray());
	}
}
