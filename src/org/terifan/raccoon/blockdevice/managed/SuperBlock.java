package org.terifan.raccoon.blockdevice.managed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import org.terifan.raccoon.document.Marshaller;
import org.terifan.raccoon.security.messagedigest.SHA3;


class SuperBlock
{
	private final static int DIGEST_LENGTH = 64;
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
		aBlockDevice.readBlock(aBlockIndex, buffer, 0, buffer.length, createBlockKey(aBlockIndex));

		SHA3 sha = new SHA3(512);
		sha.update(buffer, 0, buffer.length - DIGEST_LENGTH);

		byte[] found = sha.digest();
		byte[] expected = Arrays.copyOfRange(buffer, buffer.length - DIGEST_LENGTH, buffer.length);

		if (!Arrays.equals(found, expected))
		{
			throw new DeviceException("Checksum error at block index " + aBlockIndex);
		}

		try (Marshaller marshaller = new Marshaller(new ByteArrayInputStream(buffer)))
		{
			mGeneration = marshaller.read();
			mCreateTime = marshaller.read();
			mChangedTime = marshaller.read();
			mSpaceMapBlockPointer.unmarshalDoc(marshaller.read());
			Document metadata = marshaller.read();

			return metadata;
		}
	}


	public void write(PhysicalBlockDevice aBlockDevice, long aBlockIndex, Document aMetadata) throws IOException
	{
		mChangedTime = System.currentTimeMillis();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (Marshaller marshaller = new Marshaller(baos))
		{
			marshaller.write(mGeneration);
			marshaller.write(mCreateTime);
			marshaller.write(mChangedTime);
			marshaller.write(mSpaceMapBlockPointer.marshalDoc());
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

		byte[] digest = sha.digest();
		System.arraycopy(digest, 0, buffer, buffer.length - DIGEST_LENGTH, DIGEST_LENGTH);

		aBlockDevice.writeBlock(aBlockIndex, buffer, 0, buffer.length, createBlockKey(aBlockIndex));
	}


	public int[] createBlockKey(long aBlockIndex)
	{
		return new int[]
		{
			0, 0, 0, (int)aBlockIndex
		};
	}
}
