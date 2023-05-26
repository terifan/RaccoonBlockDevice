package org.terifan.raccoon.blockdevice.managed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import org.terifan.raccoon.document.StreamMarshaller;
import org.terifan.raccoon.security.messagedigest.SHA3;


class SuperBlock
{
	private final static int DIGEST_LENGTH = 64;
	final static int BLOCK_SIZE = 4096;

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


	public Document read(PhysicalBlockDevice aBlockDevice, int aIndex) throws IOException
	{
		int blockIndex = aIndex * BLOCK_SIZE / aBlockDevice.getBlockSize();

		byte[] buffer = new byte[BLOCK_SIZE];
		aBlockDevice.readBlock(blockIndex, buffer, 0, buffer.length, createBlockKey(blockIndex));

		SHA3 sha = new SHA3(512);
		sha.update(buffer, 0, buffer.length - DIGEST_LENGTH);

		byte[] found = sha.digest();
		byte[] expected = Arrays.copyOfRange(buffer, buffer.length - DIGEST_LENGTH, buffer.length);

		if (!Arrays.equals(found, expected))
		{
			throw new DeviceException("Checksum error in SuperBlock #" + aIndex);
		}

		try (StreamMarshaller marshaller = new StreamMarshaller(new ByteArrayInputStream(buffer)))
		{
			mGeneration = marshaller.read();
			mCreateTime = marshaller.read();
			mChangedTime = marshaller.read();
			mSpaceMapBlockPointer = new BlockPointer().putAll((Document)marshaller.read());
			Document metadata = marshaller.read();

			return metadata;
		}
	}


	public void write(PhysicalBlockDevice aBlockDevice, int aIndex, Document aMetadata) throws IOException
	{
		int blockIndex = aIndex * BLOCK_SIZE / aBlockDevice.getBlockSize();

		mChangedTime = System.currentTimeMillis();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamMarshaller marshaller = new StreamMarshaller(baos))
		{
			marshaller.write(mGeneration);
			marshaller.write(mCreateTime);
			marshaller.write(mChangedTime);
			marshaller.write(mSpaceMapBlockPointer);
			marshaller.write(aMetadata);
		}

		byte[] buffer = baos.toByteArray();

		if (buffer.length > BLOCK_SIZE - DIGEST_LENGTH)
		{
			throw new DeviceException("Fatal error: SuperBlock serialized too larger than " + (BLOCK_SIZE - DIGEST_LENGTH) + " bytes: " + baos.size());
		}

		buffer = Arrays.copyOfRange(buffer, 0, BLOCK_SIZE);

		SHA3 sha = new SHA3(512);
		sha.update(buffer, 0, buffer.length - DIGEST_LENGTH);
		System.arraycopy(sha.digest(), 0, buffer, buffer.length - DIGEST_LENGTH, DIGEST_LENGTH);

		aBlockDevice.writeBlock(blockIndex, buffer, 0, buffer.length, createBlockKey(blockIndex));
	}


	public int[] createBlockKey(long aBlockIndex)
	{
		return new int[]
		{
			0, 0, 0, (int)aBlockIndex
		};
	}
}
