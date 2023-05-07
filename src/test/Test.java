package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.security.random.SecureRandom;
import org.terifan.raccoon.blockdevice.LobHeader;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;


public class Test
{
	public static void main(String ... args)
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

				LobHeader header = new LobHeader();
				try (BlockAccessor blockAccessor = new BlockAccessor(dev))
				{
					try (LobByteChannel lob = new LobByteChannel(blockAccessor, header, LobOpenOption.CREATE))
					{
						lob.writeAllBytes(lobData);
					}
				}
				dev.getMetadata().put("lob", header.marshal());

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

				LobHeader header = new LobHeader(dev.getMetadata().getDocument("lob"));
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
}
