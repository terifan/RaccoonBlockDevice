package org.terifan.raccoon.blockdevice.test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.security.random.SecureRandom;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.util.LogLevel;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			Console.setPrettyColorsEnabled(true);
			Random rnd = new Random(1);
			byte[] output = new byte[90_000];
			nextBytes(rnd, output, 5000, 2500);
			nextBytes(rnd, output, 15000, 22500);
			nextBytes(rnd, output, 55000, 30000);

			MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);

//			Log.setLevel(LogLevel.DEBUG);
			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE, true, 2048, CompressorAlgorithm.DEFLATE_BEST))
				{
					lob.position(4100);
					lob.writeAllBytes(output);
				}
				dev.getMetadata().put("lob", header);
				dev.commit();
			}
			Log.setLevel(LogLevel.FATAL);

			System.out.println("-".repeat(100));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().get("lob");

				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					System.out.println(lob.getMetadata().toByteArray().length);
					System.out.println(lob.getMetadata());
					System.out.println(lob);
					lob.position(4100);
					byte[] input = lob.readAllBytes();
					if (!Arrays.equals(input, Arrays.copyOfRange(output, 0, input.length)))
					{
						Log.diffDump(output, 32, input);
					}
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	public static void xxmain(String... args)
	{
		for (int i = 4; i <= 4; i++)
		{
			run(i);
		}
	}


	public static void run(int aIndex)
	{
		try
		{
//			Log.setLevel(LogLevel.DEBUG);
			Random rnd = new Random(aIndex);
//			byte[] output = new byte[10_000_000];
			byte[] output = new byte[10000];
			int bufferLength = 0;

			MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE, false, 512, CompressorAlgorithm.LZJB))
				{
					for (int i = 0; i < 1; i++)
					{
//						if ((i%1000)==0)System.out.println(i);
						byte[] data = new byte[1 + rnd.nextInt(10_000)];
						boolean text = rnd.nextBoolean();
						if (text)
						{
							rnd.nextBytes(data);
						}
						lob.position(rnd.nextInt(output.length - data.length));
//						Console.printf("WRITE " + lob.position() + " +" + data.length + " " + (text?"text":"zero"));
						System.arraycopy(data, 0, output, (int)lob.position(), data.length);
						lob.writeAllBytes(data);
						bufferLength = Math.max(bufferLength, (int)lob.position());
					}
				}
				dev.getMetadata().put("lob", header);
				dev.commit();
			}

//			System.out.println(blockDevice);
//			Log.setLevel(LogLevel.DEBUG);

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
//			{
//				Document header = dev.getMetadata().get("lob");
//
//				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
//				{
//					byte[] input = lob.readAllBytes();
//					Console.printf(Arrays.equals(input, Arrays.copyOfRange(output, 0, input.length)) ? "data identical" : "data missmatch");
//					Log.diffDump(input, 32, Arrays.copyOfRange(output, 0, input.length));
//				}
//			}

//			System.out.println();
//			blockDevice.dump(64);

//			System.out.println(blockDevice);
//			Log.setLevel(LogLevel.DEBUG);

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
//			{
//				Document header = dev.getMetadata().get("lob");
//
//				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
//				{
//					byte[] input = lob.readAllBytes();
//					Console.printf(Arrays.equals(input, Arrays.copyOfRange(output, 0, input.length)) ? "data identical" : "data missmatch");
//					Log.diffDump(input, 32, Arrays.copyOfRange(output, 0, input.length));
//				}
//			}

//			System.out.println();
//			blockDevice.dump(64);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void xmain(String... args)
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
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new SecureBlockDevice(ac, new FileBlockStorage(Paths.get("d:\\test.dev"), 512, false))))
			{
				int[] blockKey = rnd.ints(4).toArray();
				long blockIndex = dev.allocBlock(directBlockData.length / dev.getBlockSize());
				dev.writeBlock(blockIndex, directBlockData, 0, directBlockData.length, blockKey);
				dev.getMetadata().put("directBlock", new Document().put("blockKey", Array.of(blockKey)).put("blockIndex", blockIndex));

				Document header = new Document();
				try (BlockAccessor blockAccessor = new BlockAccessor(dev))
				{
					try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE))
					{
						lob.writeAllBytes(lobData);
					}
				}
				dev.getMetadata().put("lob", header);

				dev.commit();
			}

//			Log.setLevel(LogLevel.DEBUG);
//			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new SecureBlockDevice(ac, new FileBlockStorage(Paths.get("d:\\test.dev"), 512, false))))
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
					try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.READ))
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


	private static void nextBytes(Random aRnd, byte[] aOutput, int aOffset, int aLength)
	{
		for (int i = 0; i < aLength; i++)
		{
			aOutput[aOffset++] = (byte)('a' + aRnd.nextInt(10));
		}
	}
}
