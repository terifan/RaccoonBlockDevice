package org.terifan.raccoon.blockdevice.managed;

import java.io.IOException;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;


public class ManagedBlockDevice implements AutoCloseable
{
	private PhysicalBlockDevice mPhysBlockDevice;
	private SuperBlock mSuperBlock;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private SpaceMap mSpaceMap;
	private Document mMetadata;
	private int mReservedBlocks;


	public ManagedBlockDevice(PhysicalBlockDevice aBlockDevice) throws IOException
	{
		if (aBlockDevice == null)
		{
			throw new IllegalArgumentException("aBlockDevice is null");
		}
		if (aBlockDevice.getBlockSize() < 512 || (aBlockDevice.getBlockSize() & (aBlockDevice.getBlockSize() - 1)) != 0)
		{
			throw new IllegalArgumentException("The block size must be power of 2 and at least 512 bytes in length.");
		}

		mPhysBlockDevice = aBlockDevice;
		mBlockSize = aBlockDevice.getBlockSize();
		mReservedBlocks = 2;
		mMetadata = new Document();
		mWasCreated = mPhysBlockDevice.size() < mReservedBlocks;

		init();
	}


	private void init() throws IOException
	{
		if (mWasCreated)
		{
			createBlockDevice();
		}
		else
		{
			loadBlockDevice();
		}
	}


	private void createBlockDevice() throws IOException
	{
		Log.i("create block device");
		Log.inc();

		mSpaceMap = new SpaceMap();
		mSuperBlock = new SuperBlock(-1L); // counter is incremented in writeSuperBlock method and we want to ensure we write block 0 before block 1

		long index = allocBlockInternal(mReservedBlocks);

		if (index != 0)
		{
			throw new IllegalStateException("The super block must be located at block index 0, was: " + index);
		}

		// write two copies of super block
		writeSuperBlock();
		writeSuperBlock();

		Log.dec();
	}


	private void loadBlockDevice() throws IOException
	{
		Log.i("load block device");
		Log.inc();

		readSuperBlock();

		mSpaceMap = new SpaceMap(mSuperBlock, this, mPhysBlockDevice);

		Log.dec();
	}


	/**
	 * @return true if the block device has pending changes requiring a commit.
	 */
	public boolean isModified()
	{
		return mModified;
	}


	/**
	 * Note: the serialized Document must fit inside the SuperBlock and be max (approx) 3500 bytes in length.
	 *
	 * @return a Document containing information about the application using the block device.
	 */
	public Document getMetadata()
	{
		return mMetadata;
	}


	/**
	 * Note: the serialized Document must fit inside the SuperBlock and be max (approx) 3500 bytes in length.
	 *
	 * @param aMetadata sets the metadata document for this BlockDevice.
	 */
	public ManagedBlockDevice setMetadata(Document aMetadata)
	{
		mMetadata.clear().putAll(aMetadata);
		return this;
	}


	/**
	 * @return the current transaction id. This value is incremented for each commit.
	 */
	public long getGeneration()
	{
		return mSuperBlock.getGeneration();
	}


	/**
	 * @return total number of blocks in this device.
	 */
	public long size()
	{
		return mPhysBlockDevice.size() - mReservedBlocks;
	}


	@Override
	public void close() throws IOException
	{
		if (mModified)
		{
			rollback();
		}

		if (mPhysBlockDevice != null)
		{
			mPhysBlockDevice.resize(mSpaceMap.getRangeMap().getLastBlockIndex());

			mPhysBlockDevice.close();
			mPhysBlockDevice = null;
		}
	}


	public int getBlockSize()
	{
		return mBlockSize;
	}


	/**
	 * Allocate a sequence of blocks on the device.
	 */
	public long allocBlock(int aBlockCount)
	{
		long blockIndex = allocBlockInternal(aBlockCount) - mReservedBlocks;

		if (blockIndex < 0)
		{
			throw new DeviceException("Illegal block index allocated.");
		}

		return blockIndex;
	}


	long allocBlockInternal(long aBlockCount)
	{
		mModified = true;

		return mSpaceMap.alloc(aBlockCount);
	}


	/**
	 * Free a block from the device.
	 *
	 * @param aBlockIndex the index of the block being freed.
	 */
	public void freeBlock(long aBlockIndex, int aBlockCount)
	{
		if (aBlockIndex < 0)
		{
			throw new DeviceException("Illegal offset: " + aBlockIndex);
		}

		freeBlockInternal(mReservedBlocks + aBlockIndex, aBlockCount);
	}


	void freeBlockInternal(long aBlockIndex, int aBlockCount)
	{
		Log.d("free block %d +%d", aBlockIndex, aBlockCount);

		mModified = true;

		mSpaceMap.free(aBlockIndex, aBlockCount);
	}


	/**
	 * Write one or more blocks to the device.
	 *
	 * @param aBlockIndex starting block index
	 * @param aBuffer buffer to be written, must be a multiple of the device block size
	 * @param aBlockKey 16 bytes (4 ints) key used to encrypt the block
	 */
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		if (aBlockIndex < 0)
		{
			throw new DeviceException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new DeviceException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(mReservedBlocks + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		mModified = true;

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mPhysBlockDevice.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		if (aBlockIndex < 0)
		{
			throw new DeviceException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new DeviceException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(aBlockIndex + mReservedBlocks, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockSize);
		Log.inc();

		mPhysBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		Log.dec();
	}


	/**
	 * Commit any pending blocks.
	 *
	 * @param aMetadata force update of metadata
	 */
	public void commit(boolean aMetadata) throws IOException
	{
		if (mModified)
		{
			Log.i("committing managed block device");
			Log.inc();

			mSpaceMap.write(mSuperBlock.getSpaceMapPointer(), this, mPhysBlockDevice);

			mPhysBlockDevice.commit(0, false);

			writeSuperBlock();

			mPhysBlockDevice.commit(1, aMetadata);

			mSpaceMap.reset();
			mWasCreated = false;
			mModified = false;

			Log.dec();
		}
	}


	/**
	 * Commit any pending blocks.
	 */
	public void commit() throws IOException
	{
		commit(false);
	}


	/**
	 * Rollback any pending blocks.
	 */
	public void rollback() throws IOException
	{
		if (mModified)
		{
			Log.i("rollbacking block device");
			Log.inc();

			mSpaceMap.reset();
			mSpaceMap.rollback();

			init();

			mModified = false;

			Log.dec();
		}
	}


	private void readSuperBlock() throws IOException
	{
		Log.d("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlock(-1L);
		SuperBlock superBlockTwo = new SuperBlock(-1L);

		Document metadataOne = superBlockOne.read(mPhysBlockDevice, 0);
		Document metadataTwo = superBlockTwo.read(mPhysBlockDevice, 1);

		if (superBlockOne.getGeneration() == superBlockTwo.getGeneration() + 1)
		{
			mSuperBlock = superBlockOne;
			mMetadata = metadataOne;

			Log.d("using super block 0");
		}
		else if (superBlockTwo.getGeneration() == superBlockOne.getGeneration() + 1)
		{
			mSuperBlock = superBlockTwo;
			mMetadata = metadataTwo;

			Log.d("using super block 1");
		}
		else
		{
			throw new IOException("BlockDevice appears to be corrupt. SuperBlock versions are illegal: " + superBlockOne.getGeneration() + " / " + superBlockTwo.getGeneration());
		}

		Log.dec();
	}


	private void writeSuperBlock() throws IOException
	{
		int index = (int)(mSuperBlock.incrementGeneration() & 1);

		Log.i("write super block %d", index);
		Log.inc();

		mSuperBlock.write(mPhysBlockDevice, index, mMetadata);

		Log.dec();
	}


	/**
	 * @return the maximum available space this block device can theoretically allocate. This value may be greater than what the underlying
	 * block device can support.
	 */
	public long getMaximumSpace()
	{
		return mSpaceMap.getRangeMap().getFreeSpace();
	}


	/**
	 * @return the size of the underlying block device, ie. size of a file acting as a block storage.
	 */
	public long getAllocatedSpace()
	{
		return mPhysBlockDevice.size();
	}


	/**
	 * @return the number of free blocks within the allocated space.
	 */
	public long getFreeSpace()
	{
		return mPhysBlockDevice.size() - mSpaceMap.getRangeMap().getUsedSpace();
	}


	/**
	 * @return the number of blocks actually used.
	 */
	public long getUsedSpace()
	{
		return mSpaceMap.getRangeMap().getUsedSpace();
	}


	/**
	 * Truncates or expands this block device to the number of blocks specified.
	 *
	 * @param aNumberOfBlocks number of blocks
	 */
	public void resize(long aNumberOfBlocks)
	{
		mPhysBlockDevice.resize(mReservedBlocks + aNumberOfBlocks);
	}


	/**
	 * Frees all blocks.
	 */
	public void clear() throws IOException
	{
		mPhysBlockDevice.resize(0);

		mSpaceMap.reset();

		createBlockDevice();
	}


	int roundUp(int aSize)
	{
		int s = mPhysBlockDevice.getBlockSize();
		return aSize + ((s - (aSize % s)) % s);
	}
}
