package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.blockdevice.physical.MemoryBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.util.LogLevel;
import org.terifan.raccoon.document.Document;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;


public class LobByteChannelNGTest
{
	@Test(enabled = false)
	public void testSomeMethod() throws IOException
	{
//		Log.setLevel(LogLevel.DEBUG);

		for (int i = 0; i < 1200; i++)
		{
			System.out.println(i);
			for (int j = 0; j < 1200; j++)
			{
				for (int k = 0; k < 1200; k++)
				{
					test(512, i, j, k);
				}
			}
		}
	}


	private void test(int aBlockSize, int... aLengths) throws IOException
	{
		byte[] chunk1 = new byte[aLengths[0]];
		byte[] chunk2 = new byte[aLengths[1]];
		byte[] chunk3 = new byte[aLengths[2]];
		new Random(1).nextBytes(chunk1);
		new Random(2).nextBytes(chunk3);

		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(512);

//		Files.deleteIfExists(Paths.get("d:\\test.dev"));

//		try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"), 512, false)))
		try (ManagedBlockDevice dev = new ManagedBlockDevice(memoryBlockDevice))
		{
			Document header = new Document();
			header.put("blockSize", aBlockSize);
			try (BlockAccessor blockAccessor = new BlockAccessor(dev))
			{
				try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE, null, null))
				{
					lob.writeAllBytes(chunk1);
					lob.writeAllBytes(chunk2);
					lob.writeAllBytes(chunk3);
				}
			}
			dev.getMetadata().put("lob", header);
			dev.commit();
		}

//		try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"), 512, false)))
		try (ManagedBlockDevice dev = new ManagedBlockDevice(memoryBlockDevice))
		{
			Document header = dev.getMetadata().getDocument("lob");
			try (BlockAccessor blockAccessor = new BlockAccessor(dev))
			{
				byte[] tmp1 = new byte[aLengths[0]];
				byte[] tmp2 = new byte[aLengths[1]];
				byte[] tmp3 = new byte[aLengths[2]];

				try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.READ, null, null))
				{
					lob.read(ByteBuffer.wrap(tmp1));
					lob.read(ByteBuffer.wrap(tmp2));
					lob.read(ByteBuffer.wrap(tmp3));
				}

//				Log.hexDump(tmp1);
//				Log.hexDump(tmp2);
//				Log.hexDump(tmp3);

				assertEquals(tmp1, chunk1);
				assertEquals(tmp2, chunk2);
				assertEquals(tmp3, chunk3);
			}
		}
	}
}
