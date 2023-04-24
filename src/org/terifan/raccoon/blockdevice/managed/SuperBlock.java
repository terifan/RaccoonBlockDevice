package org.terifan.raccoon.blockdevice.managed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.DigestException;
import java.util.Arrays;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import org.terifan.raccoon.document.Marshaller;
import org.terifan.raccoon.security.messagedigest.SHA3;


/*
 *     8  Generation
 *     8  Created date/time
 *     8  Changed date/time
 *     x  SpaceMap BlockPointer
 *  3000  Metadata
 *    64  Checksum
 */
class SuperBlock
{
	private final static int BLOCK_SIZE = 4096;

	private long mGeneration;
	private long mCreateTime;
	private long mChangedTime;
	private BlockPointer mSpaceMapBlockPointer;


	public SuperBlock(long aWriteCounter)
	{
		mCreateTime = System.currentTimeMillis();
		mSpaceMapBlockPointer = new BlockPointer();
		mGeneration = aWriteCounter;
	}


	public BlockPointer getSpaceMapPointer()
	{
		return mSpaceMapBlockPointer;
	}


	public long getGeneration()
	{
		return mGeneration;
	}


	public long incrementGeneration()
	{
		return ++mGeneration;
	}


	public long getCreatedTime()
	{
		return mCreateTime;
	}


	public long getChangedTime()
	{
		return mChangedTime;
	}


	public Document read(PhysicalBlockDevice aBlockDevice, long aBlockIndex) throws IOException
	{
		byte[] buffer = new byte[aBlockDevice.getBlockSize()];

		int[] blockKey = new int[]
		{
			0, 0, 0, (int)aBlockIndex
		};

		aBlockDevice.readBlock(aBlockIndex, buffer, 0, buffer.length, blockKey);

		SHA3 sha = new SHA3(512);
		sha.update(buffer, 0, buffer.length - sha.getDigestLength());

		if (Arrays.equals(sha.digest(), Arrays.copyOfRange(buffer, buffer.length - sha.getDigestLength(), buffer.length)))
		{
			throw new DeviceException("Checksum error at block index " + aBlockIndex);
		}

		return unmarshal(buffer);
	}


	public void write(PhysicalBlockDevice aBlockDevice, long aBlockIndex, Document aMetadata) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new DeviceException("Block at illegal offset: " + aBlockIndex);
		}

		mChangedTime = System.currentTimeMillis();

		byte[] buffer = marshal(aMetadata);

		try
		{
			SHA3 sha = new SHA3(512);
			sha.update(buffer, 0, buffer.length - sha.getDigestLength());
			sha.digest(buffer, buffer.length - sha.getDigestLength(), sha.getDigestLength());
		}
		catch (DigestException e)
		{
			throw new IOException(e);
		}

		int[] blockKey = new int[]
		{
			0, 0, 0, (int)aBlockIndex
		};

		aBlockDevice.writeBlock(aBlockIndex, buffer, 0, buffer.length, blockKey);
	}


	private byte[] marshal(Document aMetadata) throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try (Marshaller marshaller = new Marshaller(baos))
		{
			marshaller.write(mGeneration);
			marshaller.write(mCreateTime);
			marshaller.write(mChangedTime);
			marshaller.write(mSpaceMapBlockPointer.marshalDoc());
			marshaller.write(aMetadata);
		}

		if (baos.size() > BLOCK_SIZE)
		{
			throw new DeviceException("Fatal error: SuperBlock serialized too larger than one block: " + baos.size());
		}

		baos.write(new byte[BLOCK_SIZE - baos.size()]);

		return baos.toByteArray();
	}


	private Document unmarshal(byte[] aBuffer) throws IOException
	{
		try (Marshaller marshaller = new Marshaller(new ByteArrayInputStream(aBuffer)))
		{
			mGeneration = marshaller.read();
			mCreateTime = marshaller.read();
			mChangedTime = marshaller.read();
			mSpaceMapBlockPointer.unmarshalDoc(marshaller.read());
			Document metadata = marshaller.read();

			return metadata;
		}
	}
}
