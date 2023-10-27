package org.terifan.raccoon.blockdevice.test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.blockdevice.physical.MemoryBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.security.random.SecureRandom;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.util.LogLevel;
import org.terifan.raccoon.document.Array;


public class Test
{
	public static void main(String ... args)
	{
		for (int i = 10; i <= 10; i++)
		{
			run(i);
		}
	}
	public static void run(int aIndex)
	{
		try
		{
//			Log.setLevel(LogLevel.DEBUG);
			Random rnd = new Random(1);
//			byte[] buffer = new byte[10_000_000];
			byte[] buffer = new byte[10000 + aIndex*1000];
			int bufferLength = 0;

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE, null, 1, false, 512))
				{
					for (int i = 0; i < aIndex; i++)
					{
//						if ((i%1000)==0)System.out.println(i);
						byte[] data = new byte[1 + rnd.nextInt(10_000)];
						boolean text = rnd.nextBoolean();
						if (text)
						{
							rnd.nextBytes(data);
						}
						lob.position(rnd.nextInt(buffer.length - data.length));
//						System.out.println("WRITE " + lob.position() + " +" + data.length + " " + (text?"text":"zero"));
						System.arraycopy(data, 0, buffer, (int)lob.position(), data.length);
						lob.writeAllBytes(data);
						bufferLength = Math.max(bufferLength, (int)lob.position());
					}
				}
				dev.getMetadata().put("lob", header);
				dev.commit();
			}

//			System.out.println(blockDevice);
			Log.setLevel(LogLevel.DEBUG);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
			{
				Document header = dev.getMetadata().get("lob");

				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ, null))
				{
					byte[] data = lob.readAllBytes();
					System.out.println(Arrays.equals(data, Arrays.copyOfRange(buffer,0,bufferLength))?"data identical":"data missmatch");
//					Log.hexDump(data, 64, buffer);
				}
			}

//			System.out.println();

//			blockDevice.dump(64);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void xmain(String ... args)
	{
		try
		{
			Files.deleteIfExists(Paths.get("d:\\test.dev"));

			AccessCredentials ac = new AccessCredentials("password");
			SecureRandom rnd = new SecureRandom();

			byte[] directBlockData = new byte[4096 * 5];
			rnd.nextBytes(directBlockData);

			byte[] lobData = new byte[1024 * 1024 * 10];
			rnd.nextBytes(lobData);

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new SecureBlockDevice(ac, new FileBlockDevice(Paths.get("d:\\test.dev"),512,false))))
			{
				int[] blockKey = rnd.ints(4).toArray();
				long blockIndex = dev.allocBlock(directBlockData.length / dev.getBlockSize());
				dev.writeBlock(blockIndex, directBlockData, 0, directBlockData.length, blockKey);
				dev.getMetadata().put("directBlock", new Document().put("blockKey", Array.of(blockKey)).put("blockIndex", blockIndex));

				Document header = new Document();
				try (BlockAccessor blockAccessor = new BlockAccessor(dev))
				{
					try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE, null))
					{
						lob.writeAllBytes(lobData);
					}
				}
				dev.getMetadata().put("lob", header);

				dev.commit();
			}

//			Log.setLevel(LogLevel.DEBUG);

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new SecureBlockDevice(ac, new FileBlockDevice(Paths.get("d:\\test.dev"),512,false))))
			{
//				System.out.println(dev.getAllocatedSpace());
//				System.out.println(dev.getFreeSpace());
//				System.out.println(dev.size());
//				System.out.println(dev.getUsedSpace());
//				System.out.println(dev.getTransactionId());
//				System.out.println(dev.getBlockSize());
				System.out.println(dev.getMetadata());
				System.out.println("-".repeat(100));

				Document db = dev.getMetadata().getDocument("directBlock");
				int[] blockKey = db.getArray("blockKey").asInts();
				long blockIndex = db.getLong("blockIndex");

				byte[] in = new byte[4096 * 5];
				dev.readBlock(blockIndex, in, 0, in.length, blockKey);

				System.out.println(Arrays.equals(in, directBlockData));

				Document header = dev.getMetadata().getDocument("lob");
				try (BlockAccessor blockAccessor = new BlockAccessor(dev))
				{
					try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.READ, null))
					{
						byte[] data = lob.readAllBytes();
						System.out.println(Arrays.equals(data, lobData));
					}
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
