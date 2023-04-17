package test;

import java.nio.file.Paths;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.FileBlockDevice;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(new FileBlockDevice(Paths.get("d:\\test.dev"))))
			{
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
