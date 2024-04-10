package org.terifan.raccoon.blockdevice.managed;

import java.util.Arrays;
import java.util.HashSet;
import org.terifan.raccoon.blockdevice.RaccoonIOException;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.logging.Logger;
import org.terifan.raccoon.security.messagedigest.SHA3;
import org.terifan.raccoon.security.random.SecureRandom;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.blockdevice.BlockType;


class SpaceMap
{
	private final Logger log = Logger.getLogger();

	private final static SecureRandom PRNG = new SecureRandom();
	private HashSet<Long> mUncommittedAllocations;
	private RangeMap mPendingRangeMap;
	private RangeMap mRangeMap;


	public SpaceMap()
	{
		mUncommittedAllocations = new HashSet<>();

		mRangeMap = new RangeMap();
		mRangeMap.add(0, Long.MAX_VALUE);
		mPendingRangeMap = mRangeMap.clone();
	}


	public SpaceMap(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, BlockStorage aBlockDeviceDirect)
	{
		mUncommittedAllocations = new HashSet<>();

		mRangeMap = read(aSuperBlock, aBlockDevice, aBlockDeviceDirect);

		mPendingRangeMap = mRangeMap.clone();
	}


	public RangeMap getRangeMap()
	{
		return mRangeMap;
	}


	public long alloc(long aBlockCount)
	{
		long blockIndex = mRangeMap.next(aBlockCount);

		if (blockIndex < 0)
		{
			return -1;
		}

		log.t("alloc block {} +{}", blockIndex, aBlockCount);

		for (int i = 0; i < aBlockCount; i++)
		{
			mUncommittedAllocations.add(blockIndex + i);
		}

		mPendingRangeMap.remove(blockIndex, aBlockCount);

		return blockIndex;
	}


	public void free(long aBlockIndex, long aBlockCount)
	{
		long blockIndex = aBlockIndex;

		for (int i = 0; i < aBlockCount; i++)
		{
			if (mUncommittedAllocations.remove(blockIndex + i))
			{
				mRangeMap.add(blockIndex + i, 1);
			}
		}

		mPendingRangeMap.add(blockIndex, aBlockCount);
	}


	public void assertUsed(long aBlockIndex, long aBlockCount)
	{
		if (!mRangeMap.isFree((int)aBlockIndex, aBlockCount))
		{
			throw new RaccoonIOException("Range not allocated: " + aBlockIndex + " +" + aBlockCount);
		}
	}


	public void rollback()
	{
		mPendingRangeMap = mRangeMap.clone();
	}


	public void reset()
	{
		mUncommittedAllocations.clear();
	}


	public void write(BlockPointer aSpaceMapBlockPointer, ManagedBlockDevice aBlockDevice, BlockStorage aBlockDeviceDirect)
	{
		log.d("write space map");
		log.inc();

		int blockSize = aBlockDevice.getBlockSize();

		if (aSpaceMapBlockPointer.getAllocatedSize() > 0)
		{
			aBlockDevice.freeBlockInternal(aSpaceMapBlockPointer.getBlockIndex0(), aSpaceMapBlockPointer.getAllocatedSize() / blockSize);
		}

		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockSize);

		mPendingRangeMap.marshal(buffer);

		int allocSize = aBlockDevice.roundUp(buffer.position());

		long blockIndex = aBlockDevice.allocBlockInternal(allocSize / blockSize);
		int[] blockKey = PRNG.ints(4).toArray();

		aSpaceMapBlockPointer.setCompressionAlgorithm(CompressorAlgorithm.NONE.ordinal());
		aSpaceMapBlockPointer.setBlockType(BlockType.SPACEMAP);
		aSpaceMapBlockPointer.setAllocatedSize(allocSize);
		aSpaceMapBlockPointer.setBlockIndex0(blockIndex);
		aSpaceMapBlockPointer.setLogicalSize(buffer.position());
		aSpaceMapBlockPointer.setPhysicalSize(buffer.position());
		aSpaceMapBlockPointer.setChecksumAlgorithm((byte)0); // not used
		aSpaceMapBlockPointer.setChecksum(SHA3.hash128_512(buffer.array(), 0, buffer.position(), aSpaceMapBlockPointer.getGeneration()));
		aSpaceMapBlockPointer.setBlockKey(blockKey);

		// Pad buffer to block size
		buffer.capacity(allocSize);

		aBlockDeviceDirect.writeBlock(blockIndex, buffer.array(), 0, buffer.capacity(), blockKey);

		mRangeMap = mPendingRangeMap.clone();

		log.dec();
	}


	private RangeMap read(SuperBlock aSuperBlock, ManagedBlockDevice aBlockDevice, BlockStorage aBlockDeviceDirect)
	{
		BlockPointer blockPointer = aSuperBlock.getSpaceMapPointer();

		log.d("read space map {} +{} (bytes used {})", blockPointer.getBlockIndex0(), blockPointer.getAllocatedSize() / aBlockDevice.getBlockSize(), blockPointer.getLogicalSize());
		log.inc();

		RangeMap rangeMap = new RangeMap();

		if (blockPointer.getAllocatedSize() == 0)
		{
			// all blocks are free in this device
			rangeMap.add(0, Long.MAX_VALUE);
		}
		else
		{
			if (blockPointer.getBlockIndex0() < 0)
			{
				throw new RaccoonIOException("Block at illegal offset: " + blockPointer.getBlockIndex0());
			}

			int blockSize = aBlockDevice.getBlockSize();

			ByteArrayBuffer buffer = ByteArrayBuffer.alloc(blockPointer.getAllocatedSize());

			aBlockDeviceDirect.readBlock(blockPointer.getBlockIndex0(), buffer.array(), 0, blockPointer.getAllocatedSize(), blockPointer.getBlockKey());

			int[] checksum = SHA3.hash128_512(buffer.array(), 0, blockPointer.getLogicalSize(), blockPointer.getGeneration());

			if (!Arrays.equals(blockPointer.getChecksum(), checksum))
			{
				throw new RaccoonIOException("Checksum error at block index ");
			}

			buffer.limit(blockPointer.getLogicalSize());

			rangeMap.unmarshal(buffer);

			rangeMap.remove((int)blockPointer.getBlockIndex0(), blockPointer.getAllocatedSize() / blockSize);
		}

		log.dec();

		return rangeMap;
	}
}
