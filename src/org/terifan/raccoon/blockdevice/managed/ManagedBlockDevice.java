package org.terifan.raccoon.blockdevice.managed;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.DeviceException;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;


public class ManagedBlockDevice implements AutoCloseable
{
	private final static int RESERVED_BLOCKS = 2;

	private PhysicalBlockDevice mPhysBlockDevice;
	private SuperBlock mSuperBlock;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private boolean mDoubleCommit;
	private SpaceMap mSpaceMap;
	private Document mMetadata;


	public ManagedBlockDevice(PhysicalBlockDevice aBlockDevice)
	{
		if (aBlockDevice == null)
		{
			throw new IllegalArgumentException("aBlockDevice is null");
		}
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("The block device must have 512 byte block size or larger.");
		}

		mPhysBlockDevice = aBlockDevice;
		mBlockSize = aBlockDevice.getBlockSize();
		mMetadata = new Document();
		mWasCreated = mPhysBlockDevice.size() < RESERVED_BLOCKS;
		mDoubleCommit = true;

		init();
	}


	private void init()
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


	private void createBlockDevice()
	{
		Log.i("create block device");
		Log.inc();

		mSpaceMap = new SpaceMap();
		mSuperBlock = new SuperBlock(-1L); // counter is incremented in writeSuperBlock method and we want to ensure we write block 0 before block 1

		long index = allocBlockInternal(2);

		if (index != 0)
		{
			throw new IllegalStateException("The super block must be located at block index 0, was: " + index);
		}

		// write two copies of super block
		writeSuperBlock();
		writeSuperBlock();

		Log.dec();
	}


	private void loadBlockDevice()
	{
		Log.i("load block device");
		Log.inc();

		readSuperBlock();

		mSpaceMap = new SpaceMap(mSuperBlock, this, mPhysBlockDevice);

		Log.dec();
	}


	public void setDoubleCommitEnabled(boolean aDoubleCommit)
	{
		mDoubleCommit = aDoubleCommit;
	}


	/**
	 * @return true if the block device has pending changes requiring a commit.
	 */
	public boolean isModified()
	{
		return mModified;
	}


	/**
	 * Note: the serialized Document must be shorter than one block length minus 256 bytes.
	 *
	 * @return a Document containing information about the application using the block device.
	 */
	public Document getMetadata()
	{
		return mMetadata;
	}


	/**
	 * Note: the serialized Document must be shorter than one block length minus 256 bytes.
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
	public long getTransactionId()
	{
		return mSuperBlock.getTransactionId();
	}


	/**
	 * @return total number of blocks in this device.
	 */
	public long size()
	{
		return mPhysBlockDevice.size() - RESERVED_BLOCKS;
	}


	@Override
	public void close()
	{
		if (mModified)
		{
			rollback();
		}

		if (mPhysBlockDevice != null)
		{
			mPhysBlockDevice.close();
			mPhysBlockDevice = null;
		}
	}


//	public void forceClose()
//	{
//		mSpaceMap.reset();
//
//		if (mPhysBlockDevice != null)
//		{
//			mPhysBlockDevice.forceClose();
//			mPhysBlockDevice = null;
//		}
//	}


	public int getBlockSize()
	{
		return mBlockSize;
	}


	/**
	 * Allocate a sequence of blocks on the device.
	 */
	public long allocBlock(int aBlockCount)
	{
		long blockIndex = allocBlockInternal(aBlockCount) - RESERVED_BLOCKS;

		if (blockIndex < 0)
		{
			throw new DeviceException("Illegal block index allocated.");
		}

		return blockIndex;
	}


	long allocBlockInternal(int aBlockCount)
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

		freeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBlockCount);
	}


	void freeBlockInternal(long aBlockIndex, int aBlockCount)
	{
		Log.d("free block %d +%d", aBlockIndex, aBlockCount);

		mModified = true;

		mSpaceMap.free(aBlockIndex, aBlockCount);
	}


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

		writeBlockInternal(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
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

		readBlockInternal(aBlockIndex + RESERVED_BLOCKS, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
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
	public void commit(boolean aMetadata)
	{
		if (mModified)
		{
			Log.i("committing managed block device");
			Log.inc();

			mSpaceMap.write(mSuperBlock.getSpaceMapPointer(), this, mPhysBlockDevice);

			if (mDoubleCommit) // enabled by default
			{
				// commit twice since write operations on disk may occur out of order ie. superblock may be written before spacemap even
				// tough calls made in reverse order
				mPhysBlockDevice.commit(false);
			}

			writeSuperBlock();

			mPhysBlockDevice.commit(aMetadata);

			mSpaceMap.reset();
			mWasCreated = false;
			mModified = false;

			Log.dec();
		}
	}


	/**
	 * Commit any pending blocks.
	 */
	public void commit()
	{
		commit(false);
	}


	/**
	 * Rollback any pending blocks.
	 */
	public void rollback()
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


	private void readSuperBlock()
	{
		Log.d("read super block");
		Log.inc();

		SuperBlock superBlockOne = new SuperBlock(mPhysBlockDevice, 0L, -1L);
		SuperBlock superBlockTwo = new SuperBlock(mPhysBlockDevice, 1L, -1L);

		if (superBlockOne.getTransactionId() == superBlockTwo.getTransactionId() + 1)
		{
			mSuperBlock = superBlockOne;

			Log.d("using super block 0");
		}
		else if (superBlockTwo.getTransactionId() == superBlockOne.getTransactionId() + 1)
		{
			mSuperBlock = superBlockTwo;

			Log.d("using super block 1");
		}
		else
		{
			throw new IllegalStateException("Database appears to be corrupt. SuperBlock versions are illegal: " + superBlockOne.getTransactionId() + " / " + superBlockTwo.getTransactionId());
		}

		mMetadata = mSuperBlock.getMetadata();

		Log.dec();
	}


	private void writeSuperBlock()
	{
		mSuperBlock.incrementTransactionId();

		long pageIndex = mSuperBlock.getTransactionId() & 1L;

		Log.i("write super block %d", pageIndex);
		Log.inc();

		mSuperBlock.write(mPhysBlockDevice, pageIndex, mMetadata);

		Log.dec();
	}


//	@Override
//	public String toString()
//	{
//		return mSpaceMap.getRangeMap().toString();
//	}
//
//
//	/**
//	 * @return the space map layout as a String (ranges of free blocks). If the space map is fragmented this may be a long String.
//	 */
//	public String getSpaceMap()
//	{
//		return mSpaceMap.getRangeMap().toString();
//	}
//
//
//	public RangeMap getRangeMap()
//	{
//		return mSpaceMap.getRangeMap();
//	}


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
		mPhysBlockDevice.resize(RESERVED_BLOCKS + aNumberOfBlocks);
	}


	/**
	 * Frees all blocks.
	 */
	public void clear()
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
