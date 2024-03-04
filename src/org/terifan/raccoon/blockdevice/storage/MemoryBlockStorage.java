package org.terifan.raccoon.blockdevice.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.RaccoonIOException;


public class MemoryBlockStorage implements BlockStorage
{
	private final Logger log = Logger.getLogger();

	private SortedMap<Long, byte[]> mStorage = Collections.synchronizedSortedMap(new TreeMap<>());
	private int mBlockSize;


	public MemoryBlockStorage()
	{
		this(4096);
	}


	public MemoryBlockStorage(int aBlockSize)
	{
		mBlockSize = aBlockSize;
	}


	public MemoryBlockStorage(int aBlockSize, Map<Long, byte[]> aStorage)
	{
		mBlockSize = aBlockSize;
		mStorage.putAll(aStorage);
	}


	public void setBlockSize(int aBlockSize)
	{
		if (mBlockSize != aBlockSize)
		{
			mStorage.clear();
		}
		mBlockSize = aBlockSize;
	}


	@Override
	public boolean isReadOnly()
	{
		return false;
	}


	public Map<Long, byte[]> getStorage()
	{
		return mStorage;
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		log.d("write block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

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
		log.d("read block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

		while (aBufferLength > 0)
		{
			byte[] block = mStorage.get(aBlockIndex);

			if (block != null)
			{
				System.arraycopy(block, 0, aBuffer, aBufferOffset, mBlockSize);
			}
			else
			{
				throw new RaccoonIOException("Reading a free block: " + aBlockIndex);
			}

			aBlockIndex++;
			aBufferOffset += mBlockSize;
			aBufferLength -= mBlockSize;
		}
	}


	@Override
	public void commit(int aIndex, boolean aMetadata)
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


	@Override
	public String toString()
	{
		return "MemoryBlockDevice{blockSize=" + mBlockSize + ", count=" + mStorage.size() + ", range=[" + mStorage.firstKey() + ", " + mStorage.lastKey() + "]}";
	}


	public void dump()
	{
		for (Entry<Long,byte[]> entry : mStorage.entrySet())
		{
			log.i("Block #{}:", entry.getKey());
			log.hexDump(Level.INFO, entry.getValue(), 32);
		}
	}


	public void dump(int aWidth)
	{
		for (Entry<Long,byte[]> entry : mStorage.entrySet())
		{
			log.i("Block #{}:", entry.getKey());
			log.hexDump(Level.INFO, entry.getValue(), aWidth);
		}
	}
}
