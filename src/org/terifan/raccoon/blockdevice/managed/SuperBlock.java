package org.terifan.raccoon.blockdevice.managed;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.security.messagedigest.MurmurHash3;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import org.terifan.raccoon.security.random.SecureRandom;


/*
 *    16  IV
 *     2  Format version
 *     8  Created date/time
 *     8  Changed date/time
 *    16  Transaction counter
 *   128  SpaceMap BlockPointer
 *  3838  Metadata
 *    32  Checksum
 */
class SuperBlock
{
	private final static byte FORMAT_VERSION = 1;
	private final static int IV_SIZE = 16;
	private final static int CHECKSUM_SIZE = 16;
	private final static int OVERHEAD = 205; // = IV_SIZE + 1 + 8 + 8 + 8 + BlockPointer.SIZE + 2 + CHECKSUM_SIZE
	private final static SecureRandom PRNG = new SecureRandom();

	private int mFormatVersion;
	private long mCreateTime;
	private long mModifiedTime;
	private long mTransactionId;
	private BlockPointer mSpaceMapPointer;


	public SuperBlock(long aTransactionId)
	{
		mFormatVersion = FORMAT_VERSION;
		mCreateTime = System.currentTimeMillis();
		mSpaceMapPointer = new BlockPointer();
		mTransactionId = aTransactionId;
	}


	public BlockPointer getSpaceMapPointer()
	{
		return mSpaceMapPointer;
	}


	public long getTransactionId()
	{
		return mTransactionId;
	}


	public long nextTransactionId()
	{
		return ++mTransactionId;
	}


	public int getFormatVersion()
	{
		return mFormatVersion;
	}


	public long getCreateTime()
	{
		return mCreateTime;
	}


	public void setCreateTime(long aCreateTime)
	{
		mCreateTime = aCreateTime;
	}


	public long getModifiedTime()
	{
		return mModifiedTime;
	}


	public void setModifiedTime(long aModifiedTime)
	{
		mModifiedTime = aModifiedTime;
	}


	public Document read(PhysicalBlockDevice aBlockDevice, long aBlockIndex)
	{
		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).readBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.readBlock(aBlockIndex, buffer.array(), 0, buffer.capacity(), null);
		}

		long[] hash = MurmurHash3.hash128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, 0);

		buffer.position(0);

		for (long i : hash)
		{
			if (buffer.readInt64() != i)
			{
				throw new DeviceException("Checksum error at block index " + aBlockIndex);
			}
		}

		return unmarshal(buffer);
	}


	public void write(PhysicalBlockDevice aBlockDevice, long aBlockIndex, Document aMetadata)
	{
		if (aBlockIndex < 0)
		{
			throw new DeviceException("Block at illegal offset: " + aBlockIndex);
		}

		mModifiedTime = System.currentTimeMillis();

		int blockSize = aBlockDevice.getBlockSize();

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize, true);
		buffer.position(CHECKSUM_SIZE); // reserve space for checksum

		marshal(buffer, aMetadata);

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			PRNG.nextBytes(buffer.array(), buffer.position(), buffer.remaining() - IV_SIZE);
		}

		long[] hash = MurmurHash3.hash128(buffer.array(), CHECKSUM_SIZE, blockSize - CHECKSUM_SIZE - IV_SIZE, 0);

		buffer.position(0);
		for (long i : hash)
		{
			buffer.writeInt64(i);
		}

		if (aBlockDevice instanceof SecureBlockDevice)
		{
			((SecureBlockDevice)aBlockDevice).writeBlockWithIV(aBlockIndex, buffer.array(), 0, blockSize);
		}
		else
		{
			aBlockDevice.writeBlock(aBlockIndex, buffer.array(), 0, blockSize, new int[4]);
		}
	}


	private void marshal(ByteArrayBuffer aBuffer, Document aMetadata)
	{
		byte[] metadata = aMetadata.toByteArray();

		if (OVERHEAD + metadata.length > aBuffer.capacity())
		{
			throw new DeviceException("Application metadata exeeds maximum size: limit: " + (aBuffer.capacity() - OVERHEAD) + ", metadata: " + metadata.length);
		}

		aBuffer.writeInt8(mFormatVersion);
		aBuffer.writeInt64(mCreateTime);
		aBuffer.writeInt64(mModifiedTime);
		aBuffer.writeInt64(mTransactionId);
		mSpaceMapPointer.marshal(aBuffer);
		aBuffer.writeInt16(metadata.length);
		aBuffer.write(metadata);
	}


	private Document unmarshal(ByteArrayBuffer aBuffer)
	{
		mFormatVersion = aBuffer.readInt8();

		if (mFormatVersion != FORMAT_VERSION)
		{
			throw new UnsupportedVersionException("Data format is not supported: was " + mFormatVersion + ", expected " + FORMAT_VERSION);
		}

		mCreateTime = aBuffer.readInt64();
		mModifiedTime = aBuffer.readInt64();
		mTransactionId = aBuffer.readInt64();
		mSpaceMapPointer.unmarshal(aBuffer);
		return new Document().fromByteArray(aBuffer.read(new byte[aBuffer.readInt16()]));
	}
}
