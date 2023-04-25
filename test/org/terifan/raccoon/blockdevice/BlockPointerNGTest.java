package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.Log;
import org.testng.annotations.Test;


public class BlockPointerNGTest
{
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
	}


	@Test
	public void testMarshalDocument()
	{
		BlockPointer bp = new BlockPointer()
			.setBlockType(BlockType.FREE)
			.setBlockLevel((byte)1)
			.setCompressionAlgorithm((byte)2)
			.setChecksumAlgorithm((byte)3)
			.setAllocatedSize(100)
			.setLogicalSize(75)
			.setPhysicalSize(50)
			.setBlockIndex0(3421654)
			.setGeneration(3216)
			.setBlockKey(new int[]{0x88888888,0x99999999,0xaaaaaaaa,0xbbbbbbbb})
			.setChecksum(new int[]{0xcccccccc,0xcccccccc,0xdddddddd,0xdddddddd})
			;

		System.out.println(bp.marshalDoc());
		System.out.println(bp.marshalDoc().toByteArray().length);
		Log.hexDump(bp.marshalDoc().toByteArray());

//		System.out.println(bp.marshalDoc2());
//		System.out.println(bp.marshalDoc2().toByteArray().length);
//		Log.hexDump(bp.marshalDoc2().toByteArray());
//
//		System.out.println(bp.marshalDoc3());
//		System.out.println(bp.marshalDoc3().toByteArray().length);
//		Log.hexDump(bp.marshalDoc3().toByteArray());
//
		System.out.println(bp.marshalDoc4());
		System.out.println(bp.marshalDoc4().toByteArray().length);
		Log.hexDump(bp.marshalDoc4().toByteArray());

//		System.out.println(bp.marshalDoc5());
//		System.out.println(bp.marshalDoc5().toByteArray().length);
//		Log.hexDump(bp.marshalDoc5().toByteArray());

		System.out.println(bp.marshalDoc6());
		System.out.println(bp.marshalDoc6().toByteArray().length);
		Log.hexDump(bp.marshalDoc6().toByteArray());
	}
}
