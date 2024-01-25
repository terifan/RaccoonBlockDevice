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
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;


public class Test
{
	private final static Logger log = Logger.getLogger();
	private final static Random rnd = new Random(1);


	public static void main(String ... args)
	{
		try
		{
			Logger.getLogger().setLevel(Level.OFF);

			MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> Document.of("leaf:1024,node:1024,compress=lzjb"));
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE))
				{
					byte[] data = new byte[8192];
					new Random().nextBytes(data);
					lob.writeAllBytes(data);
					lob.position(1000_000);
					lob.writeAllBytes(data);
					lob.writeAllBytes(new byte[1024 * 1024]);
				}

				dev.getMetadata().put("lob", header);
				dev.commit();

//				System.out.println(header);
			}

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					byte[] data1 = new byte[8192];
					byte[] data2 = new byte[8192];
					byte[] data3 = new byte[1024*1024];
					lob.position(0);
					lob.readAllBytes(data1);
					lob.position(1000_000);
					lob.readAllBytes(data2);
					lob.readAllBytes(data3);
					if (!Arrays.equals(data1, data2))throw new IllegalStateException();
					if (!Arrays.equals(data3, new byte[1024*1024]))throw new IllegalStateException();
				}
			}

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.APPEND))
				{
					byte[] data = new byte[8192];
					lob.position(0);
					lob.readAllBytes(data);
					lob.position(1500_000);
					lob.writeAllBytes(data);
				}

				dev.getMetadata().put("lob", header);
				dev.commit();
			}

			byte[] buffer = new byte[1024*1024*3];

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					byte[] data1 = new byte[8192];
					byte[] data2 = new byte[8192];
					byte[] data3 = new byte[8192];
					lob.position(0);
					lob.readAllBytes(data1);
					lob.position(1000_000);
					lob.readAllBytes(data2);
					lob.position(1500_000);
					lob.readAllBytes(data3);
					if (!Arrays.equals(data1, data2))throw new IllegalStateException();
					if (!Arrays.equals(data1, data3))throw new IllegalStateException();

					lob.position(0);
					byte[] tmp = lob.readAllBytes();
					System.arraycopy(tmp, 0, buffer, 0, tmp.length);

//					lob.scan();
				}
			}

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.APPEND))
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

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().computeIfAbsent("lob", k -> new Document());
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					byte[] tmp = lob.readAllBytes();
					if (!Arrays.equals(tmp, Arrays.copyOfRange(buffer,0,tmp.length)))throw new IllegalStateException();

					System.out.println("-".repeat(200));
					lob.scan();
				}
			}

			System.out.println(blockStorage.size());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	public static void xxxmain(String... args)
	{
		try
		{
			log.setLevel(Level.ALL);

			byte[] a = nextBytes(4000);
			byte[] b = nextBytes(4000);
			byte[] c = nextBytes(4000);
			byte[] d = nextBytes(4000);

			MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE))
				{
					lob.writeAllBytes(a);
					lob.flush();
					lob.scan();
					lob.writeAllBytes(b);
					lob.flush();
					lob.scan();
				}
				dev.getMetadata().put("lob", header);
				System.out.println(header);
				dev.commit();
			}

			System.out.println("-".repeat(200));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().get("lob");
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.APPEND))
				{
					lob.position(0);
//					lob.scan();
					byte[] data = lob.readAllBytes();
					System.out.println(data.length);
					System.out.println(Arrays.equals(a, Arrays.copyOfRange(data, 0, a.length)));
					System.out.println(Arrays.equals(b, Arrays.copyOfRange(data, a.length, a.length + b.length)));

					lob.position(a.length/2);
					lob.writeAllBytes(c);
					System.out.println("-".repeat(200));
					lob.scan();
				}
				dev.commit();
			}

			System.out.println("-".repeat(200));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().get("lob");
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					lob.position(0);
					lob.scan();
					byte[] data = lob.readAllBytes();
					System.out.println(data.length);
					System.out.println(Arrays.equals(Arrays.copyOfRange(a,0,a.length/2), Arrays.copyOfRange(data, 0, a.length/2)));
					System.out.println(Arrays.equals(Arrays.copyOfRange(b,b.length/2,b.length), Arrays.copyOfRange(data, a.length+b.length/2, a.length + b.length)));
					System.out.println(Arrays.equals(c, Arrays.copyOfRange(data, a.length/2, a.length/2 + c.length)));
				}
			}

			System.out.println("-".repeat(200));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().get("lob");
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.APPEND))
				{
					lob.position(20000);
					lob.writeAllBytes(new byte[4000]);
					lob.writeAllBytes(d);
				}
				dev.commit();
			}

			System.out.println("-".repeat(200));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = dev.getMetadata().get("lob");
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
				{
					lob.scan();
					byte[] data = lob.readAllBytes();
					System.out.println(data.length);
				}
				dev.commit();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void aasasmain(String... args)
	{
		try
		{
			byte[] output = new byte[90_000];
			nextBytes(output, 5000, 2500);
			nextBytes(output, 15000, 22500);
			nextBytes(output, 55000, 30000);

			MemoryBlockStorage blockStorage = new MemoryBlockStorage(512);

//			Log.setLevel(LogLevel.DEBUG);
			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE))
				{
					lob.position(2 * 1024 + 100);
					lob.writeAllBytes(output);

					lob.scan();
				}
				dev.getMetadata().put("lob", header);
//				System.out.println(header);
				dev.commit();
			}
			log.setLevel(Level.FATAL);

//			System.out.println("-".repeat(100));
//
//			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockStorage))
//			{
//				Document header = dev.getMetadata().get("lob");
//
//				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.READ))
//				{
//					System.out.println(lob.getMetadata().toByteArray().length);
//					System.out.println(lob.getMetadata());
//					System.out.println(lob);
//					lob.position(2*1024+100);
//					byte[] input = lob.readAllBytes();
//					if (!Arrays.equals(input, Arrays.copyOfRange(output, 0, input.length)))
//					{
//						Log.diffDump(output, 32, input);
//					}
//				}
//			}
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
//			byte[] output = new byte[10_000_000];
			byte[] output = new byte[10000];
			int bufferLength = 0;

			MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);

			try (ManagedBlockDevice dev = new ManagedBlockDevice(blockDevice))
			{
				Document header = new Document();
				try (LobByteChannel lob = new LobByteChannel(new BlockAccessor(dev), header, LobOpenOption.CREATE))
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


	private static byte[] nextBytes(int aLength)
	{
		return nextBytes(new byte[aLength], 0, aLength);
	}


	private static byte[] nextBytes(byte[] aOutput, int aOffset, int aLength)
	{
		for (int i = 0; i < aLength; i++)
		{
			aOutput[aOffset++] = (byte)('a' + rnd.nextInt(26));
		}
		return aOutput;
	}
}
