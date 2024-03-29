package org.terifan.raccoon.blockdevice;

import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
	@Test
	public void testSomeMethod()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.BTREE_NODE)
			.setBlockLevel(1)
			.setChecksumAlgorithm(2)
			.setCompressionAlgorithm(3)
			.setAllocatedSize(32768)
			.setLogicalSize(31264)
			.setPhysicalSize(21679)
			.setBlockIndex0(26161)
			.setGeneration(71)
			.setBlockKey(new int[]{0x88888888,0x99999999,0xaaaaaaaa,0xbbbbbbbb})
			.setChecksum(new int[]{0xcccccccc,0xdddddddd,0xeeeeeeee,0xffffffff})
			;

		System.out.println(bp.toByteArray().length);
		Logger.getLogger().hexDump(Level.INFO, bp.toByteArray());
	}
}
