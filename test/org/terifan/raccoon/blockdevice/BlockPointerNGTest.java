package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.Log;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
	@Test
	public void testMarshal()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.FREE)
			.setBlockLevel((byte)1)
			.setChecksumAlgorithm((byte)2)
			.setCompressionAlgorithm((byte)3)
			.setAllocatedSize(0x11111111)
			.setLogicalSize(0x22222222)
			.setPhysicalSize(0x33333333)
			.setBlockIndex0(0x4444444444444444L)
			.setBlockIndex1(0x5555555555555555L)
			.setBlockIndex2(0x6666666666666666L)
			.setGeneration(0x7777777777777777L)
			.setBlockKey(new int[]{0x88888888,0x99999999,0xaaaaaaaa,0xbbbbbbbb})
			.setChecksum(new int[]{0xcccccccc,0xcccccccc,0xdddddddd,0xdddddddd})
			;

		BlockPointer bp2 = new BlockPointer().unmarshal(bp.marshal());
		assertEquals(bp2.marshalDoc(), bp.marshalDoc());

		BlockPointer bp3 = new BlockPointer().unmarshalDoc(bp.marshalDoc());
		assertEquals(bp3.marshalDoc(), bp.marshalDoc());
	}

	@Test
	public void testSomeMethod()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.FREE)
			.setBlockLevel((byte)1)
			.setChecksumAlgorithm((byte)2)
			.setCompressionAlgorithm((byte)3)
			.setAllocatedSize(0x11111111)
			.setLogicalSize(0x22222222)
			.setPhysicalSize(0x33333333)
			.setBlockIndex0(0x4444444444444444L)
			.setBlockIndex1(0x5555555555555555L)
			.setBlockIndex2(0x6666666666666666L)
			.setGeneration(0x7777777777777777L)
			.setBlockKey(new int[]{0x88888888,0x99999999,0xaaaaaaaa,0xbbbbbbbb})
			.setChecksum(new int[]{0xcccccccc,0xcccccccc,0xdddddddd,0xdddddddd})
			;

		System.out.println(bp.marshalDoc());
		System.out.println(bp.marshalDoc2());
		System.out.println(bp.marshalDoc().toByteArray().length);
		System.out.println(bp.marshalDoc2().toByteArray().length);
	}
}
