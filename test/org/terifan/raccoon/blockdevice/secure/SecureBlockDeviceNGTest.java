package org.terifan.raccoon.blockdevice.secure;

import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;


public class SecureBlockDeviceNGTest
{
	@Test
	public void test1() throws IOException
	{
		MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);
		AccessCredentials accessCredentials = new AccessCredentials("password");

		byte[] out = new byte[512];
		byte[] in = new byte[512];
		int[] key = new Random(1).ints(4).toArray();

		Arrays.fill(out, (byte)65);

//		try (PhysicalBlockDevice device = blockDevice)
		try (SecureBlockDevice device = new SecureBlockDevice(accessCredentials, blockDevice))
		{
			device.writeBlock(0, out, 0, out.length, key);
		}

		blockDevice.dump();

//		try (PhysicalBlockDevice device = blockDevice)
		try (SecureBlockDevice device = new SecureBlockDevice(accessCredentials, blockDevice))
		{
			device.readBlock(0, in, 0, in.length, key);
		}

		assertEquals(out, in);
	}


	@Test
	public void testKeyAndEncryptionOptions() throws IOException
	{
		for (KeyGenerationFunction kgf : KeyGenerationFunction.values())
		{
			for (EncryptionFunction ef : EncryptionFunction.values())
			{
				for (CipherModeFunction cmf : CipherModeFunction.values())
				{
					Random rnd = new Random();

					int unitSize = 512;
					int numUnits = 32;
					int blocksPerUnit = 4;

					MemoryBlockStorage blockDevice = new MemoryBlockStorage(unitSize);

					int[][] blockKeys = new int[numUnits][4];
					for (int i = 0; i < numUnits; i++)
					{
						blockKeys[i][0] = rnd.nextInt();
						blockKeys[i][1] = rnd.nextInt();
						blockKeys[i][2] = rnd.nextInt();
						blockKeys[i][3] = rnd.nextInt();
					}

					byte[] original = new byte[numUnits * unitSize];
					rnd.nextBytes(original);

					byte[] input = original.clone();

					long t0 = System.currentTimeMillis();

					try (SecureBlockDevice device = new SecureBlockDevice(new AccessCredentials("password".toCharArray(), ef, kgf, cmf, 1), blockDevice))
					{
						for (int i = 0; i < numUnits / blocksPerUnit; i++)
						{
							device.writeBlock(blocksPerUnit * i, input, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
						}
					}

					long t1 = System.currentTimeMillis();

					assertEquals(input, original);

					byte[] output = new byte[numUnits * unitSize];

					try (SecureBlockDevice device = new SecureBlockDevice(new AccessCredentials("password".toCharArray()).setIterationCount(1), blockDevice))
					{
						for (int i = 0; i < numUnits / blocksPerUnit; i++)
						{
							device.readBlock(blocksPerUnit * i, output, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
						}
					}

					long t2 = System.currentTimeMillis();

					assertEquals(output, input);

	//				System.out.printf("%4d %4d %s %s%n", t1-t0, t2-t1, kgf, ef);
				}
			}
		}
	}
}
