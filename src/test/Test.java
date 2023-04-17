package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;
import org.terifan.raccoon.document.Document;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			Files.delete(Paths.get("d:\\test.dev"));

			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			{
				dev.allocBlock(10);
				dev.setMetadata(new Document().put("test", "xxxx"));
				dev.commit();
			}

			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			{
				System.out.println(dev.getAllocatedSpace());
				System.out.println(dev.getFreeSpace());
				System.out.println(dev.size());
				System.out.println(dev.getUsedSpace());
				System.out.println(dev.getTransactionId());
				System.out.println(dev.getMetadata());
				System.out.println(dev.getBlockSize());
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
