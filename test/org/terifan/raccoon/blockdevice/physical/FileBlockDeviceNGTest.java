package org.terifan.raccoon.blockdevice.physical;

import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class FileBlockDeviceNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		Path file = Files.createTempFile("blkdev","tmp");

		Random rnd = new Random(1);
		int s = 512;
		HashMap<Long,byte[]> blocks = new HashMap<>();
		ArrayList<Long> offsets = new ArrayList<>();

		for (int k = 0; k < 5; k++)
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockStorage(file, s)))
			{
				for (int j = 0; j < 10; j++)
				{
					for (int i = 0; i < 50; i++)
					{
						long pos = dev.allocBlock(1);
						byte[] buf = new byte[s];
						rnd.nextBytes(buf);
						blocks.put(pos, buf);
						dev.writeBlock(pos, buf, 0, s, new int[4]);
						offsets.add(pos);
					}

					for (int i = 0; i < 25; i++)
					{
						long pos = offsets.remove(rnd.nextInt(offsets.size()));
						byte[] buf = new byte[s];
						dev.readBlock(pos, buf, 0, s, new int[4]);
						assertEquals(blocks.remove(pos), buf);
						dev.freeBlock(pos, 1);
					}

					dev.commit();
				}
			}
		}

		Files.delete(file);
	}
}
