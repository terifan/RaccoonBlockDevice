package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.security.random.SecureRandom;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			Files.delete(Paths.get("d:\\test.dev"));

			AccessCredentials ac = new AccessCredentials("test");
			SecureRandom rnd = new SecureRandom();

//			int[] blockKey = null;
			int[] blockKey = rnd.ints(4).toArray();
			long blockIndex;
			byte[] out = new byte[4096 * 10];

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			try (ManagedBlockDevice dev = new ManagedBlockDevice(SecureBlockDevice.create(ac, new FileBlockDevice(Paths.get("d:\\test.dev")))))
			{
				blockIndex = dev.allocBlock(out.length / dev.getBlockSize());
				dev.setMetadata(new Document().put("test", "xxxx"));
				dev.writeBlock(blockIndex, out, 0, out.length, blockKey);
				dev.commit();
			}

//			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			try (ManagedBlockDevice dev = new ManagedBlockDevice(SecureBlockDevice.open(ac, new FileBlockDevice(Paths.get("d:\\test.dev")))))
			{
				System.out.println(dev.getAllocatedSpace());
				System.out.println(dev.getFreeSpace());
				System.out.println(dev.size());
				System.out.println(dev.getUsedSpace());
				System.out.println(dev.getTransactionId());
				System.out.println(dev.getMetadata());
				System.out.println(dev.getBlockSize());

				byte[] in = new byte[dev.getBlockSize() * 10];
				dev.readBlock(blockIndex, in, 0, in.length, blockKey);

				System.out.println(Arrays.equals(in, out));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
