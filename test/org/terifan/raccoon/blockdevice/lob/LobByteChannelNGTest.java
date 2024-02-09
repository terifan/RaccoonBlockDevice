package org.terifan.raccoon.blockdevice.lob;

import org.terifan.raccoon.blockdevice.lob.LobOpenOption;
import org.terifan.raccoon.blockdevice.lob.LobByteChannel;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.document.Document;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;


public class LobByteChannelNGTest
{
	@Test
	public void testSomeMethod1() throws IOException
	{
		Logger.getLogger().setLevel(Level.OFF);

		MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);

		try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
		{
			Document header = new Document();
			try (BlockAccessor blockAccessor = new BlockAccessor(dev))
			{
				try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE, null))
				{
					byte[] data = new byte[1024];
					new Random().nextBytes(data);
					lob.writeAllBytes(data);
					lob.position(1000_000);
					lob.writeAllBytes(data);
					lob.writeAllBytes(new byte[1024 * 1024]);
				}
			}

			dev.getMetadata().put("lob", header);
			dev.commit();
		}

		try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
		{
			try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), dev.getMetadata().get("lob"), LobOpenOption.READ, null))
			{
				byte[] data1 = new byte[1024];
				byte[] data2 = new byte[1024];
				byte[] data3 = new byte[1024 * 1024];
				lob.position(0);
				lob.readAllBytes(data1);
				lob.position(1000_000);
				lob.readAllBytes(data2);
				lob.readAllBytes(data3);
				assertEquals(data1, data2);
				assertEquals(data3, new byte[1024 * 1024]);

//				lob.scan();
			}
			dev.commit();
		}

		System.out.println(blockStorage.size());
	}


	@Test
	public void testRandomWritesWithAlotOfHoles() throws IOException
	{
		Logger.getLogger().setLevel(Level.OFF);

		Random rnd = new Random(1);
		MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);
		byte[] buffer = new byte[1024 * 1024 * 10];

		for (int test = 0; test < 10; test++)
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());

				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.APPEND, null))
				{
					for (int i = 0; i < 1000; i++)
					{
						byte[] data = new byte[rnd.nextInt(50000)];
						if (i < 500 && rnd.nextInt(100) < 50)
						{
							rnd.nextBytes(data);
						}

						int pos = rnd.nextInt(buffer.length - data.length);
						lob.position(pos);
						lob.writeAllBytes(data);

						System.arraycopy(data, 0, buffer, pos, data.length);
					}
				}

				dev.getMetadata().put("lob", header);
				dev.commit();
			}
		}

		try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
		{
			Document header = dev.getMetadata().get("lob");
			try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ, null))
			{
				byte[] tmp = lob.readAllBytes();

				assertEquals(buffer, Arrays.copyOfRange(tmp, 0, buffer.length));

//				lob.scan();
			}
		}
	}
}
