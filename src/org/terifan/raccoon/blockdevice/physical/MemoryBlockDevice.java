package org.terifan.raccoon.blockdevice.physical;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.util.Log;


public class MemoryBlockDevice implements PhysicalBlockDevice
{
	private final SortedMap<Long, byte[]> mStorage = Collections.synchronizedSortedMap(new TreeMap<>());
	private final int mBlockSize;


	public MemoryBlockDevice()
	{
		this(4096);
	}


	public MemoryBlockDevice(int aBlockSize)
	{
		mBlockSize = aBlockSize;
	}


	public Map<Long, byte[]> getStorage()
	{
		return mStorage;
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		while (aBufferLength > 0)
		{
			mStorage.put(aBlockIndex, Arrays.copyOfRange(aBuffer, aBufferOffset, aBufferOffset + mBlockSize));

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);

		while (aBufferLength > 0)
		{
			byte[] block = mStorage.get(aBlockIndex);

			if (block != null)
			{
				System.arraycopy(block, 0, aBuffer, aBufferOffset, mBlockSize);
			}
			else
			{
				throw new DeviceException("Reading a free block: " + aBlockIndex);
			}

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void commit(boolean aMetadata)
	{
	}


	@Override
	public void close()
	{
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public long size()
	{
		return mStorage.isEmpty() ? 0L : mStorage.lastKey() + 1;
	}


	@Override
	public synchronized void resize(long aNumberOfBlocks)
	{
		Long[] offsets = mStorage.keySet().toArray(new Long[mStorage.size()]);

		for (Long offset : offsets)
		{
			if (offset >= aNumberOfBlocks)
			{
				mStorage.remove(offset);
			}
		}
	}


	public void dump()
	{
		for (Entry<Long,byte[]> entry : mStorage.entrySet())
		{
			Log.out.println("Block #" + entry.getKey() + ":");
			Log.hexDump(entry.getValue());
		}
	}
}
