package org.terifan.raccoon.blockdevice.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.BlockDeviceOpenOption;
import org.terifan.raccoon.blockdevice.RaccoonIOException;


public class MemoryBlockStorage extends BlockStorage
{
	private final Logger log = Logger.getLogger();

	private SortedMap<Long, byte[]> mStorage;
	private int mBlockSize;
	private boolean mReadOnly;


	public MemoryBlockStorage()
	{
		this(4096);
	}


	public MemoryBlockStorage(int aBlockSize)
	{
		mBlockSize = aBlockSize;
	}


	@Override
	public MemoryBlockStorage open(BlockDeviceOpenOption aOptions)
	{
		setOpenState();

		if (mStorage == null || aOptions == BlockDeviceOpenOption.REPLACE)
		{
			mStorage = Collections.synchronizedSortedMap(new TreeMap<>());
		}
		mReadOnly = aOptions == BlockDeviceOpenOption.READ_ONLY;
		return this;
	}


	@Override
	public boolean isReadOnly()
	{
		return mReadOnly;
	}


	public Map<Long, byte[]> getStorage()
	{
		return mStorage;
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assertOpen();

		if (mReadOnly)
		{
			throw new IllegalStateException();
		}

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
		assertOpen();

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
		assertOpen();
	}


	@Override
	public void close()
	{
		setClosedState();
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public long size()
	{
		assertOpen();

		return mStorage.isEmpty() ? 0L : mStorage.lastKey() + 1;
	}


	@Override
	public synchronized void resize(long aNumberOfBlocks)
	{
		assertOpen();

		if (mReadOnly)
		{
			throw new IllegalStateException();
		}

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
		for (Entry<Long, byte[]> entry : mStorage.entrySet())
		{
			log.i("Block #{}:", entry.getKey());
			log.hexDump(Level.INFO, entry.getValue(), 32);
		}
	}


	public void dump(int aWidth)
	{
		for (Entry<Long, byte[]> entry : mStorage.entrySet())
		{
			log.i("Block #{}:", entry.getKey());
			log.hexDump(Level.INFO, entry.getValue(), aWidth);
		}
	}
}
