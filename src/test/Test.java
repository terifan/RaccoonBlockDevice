package test;

import java.nio.file.Paths;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			{
				dev.resize(0);

				long pos1 = dev.allocBlock(1);
				long pos2 = dev.allocBlock(1);
				dev.commit(); // allocs 2
				long pos3 = dev.allocBlock(1);
				long pos4 = dev.allocBlock(1);
				dev.commit(); // allocs 5, frees 2
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
