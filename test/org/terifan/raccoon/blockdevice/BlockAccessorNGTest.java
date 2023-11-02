package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.DataProvider;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;


public class BlockAccessorNGTest
{
	@Test
	public void testWriteReadFreeSingleBlock() throws IOException
	{
		int length = 3 * 4096;
		byte[] in = new byte[100 + length + 100];
		new Random().nextBytes(in);

		MemoryBlockStorage blockDevice = new MemoryBlockStorage(4096);

		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
		BlockAccessor blockAccessor = new BlockAccessor(managedBlockDevice, true);
		BlockPointer blockPointer = blockAccessor.writeBlock(in, 100, length, BlockType.BTREE_NODE, 0, CompressorAlgorithm.ZLE);
		managedBlockDevice.commit();

		assertEquals(2 + 1 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 1 spacemap + 3 data
		assertEquals(0, managedBlockDevice.getFreeSpace());

		byte[] out = blockAccessor.readBlock(blockPointer);
		byte[] expcted = Arrays.copyOfRange(in, 100, 100 + length);

		assertEquals(expcted, out);

		blockAccessor.freeBlock(blockPointer);
		managedBlockDevice.commit();

		assertEquals(2 + 2 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 2 spacemap + 3 data
		assertEquals(1 + 3, managedBlockDevice.getFreeSpace()); // 1 spacemap + 3 data
	}


	@DataProvider
	private Object[][] cacheSize()
	{
		return new Object[][]{
			{false},
			{true}
		};
	}
}
