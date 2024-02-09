package org.terifan.raccoon.blockdevice.managed;

import java.io.IOException;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.RaccoonIOException;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;


public class ManagedBlockDevice implements AutoCloseable
{
	private final Logger log = Logger.getLogger();

	private BlockStorage mBlockStorage;
	private SuperBlock mSuperBlock;
	private int mBlockSize;
	private boolean mModified;
	private boolean mWasCreated;
	private SpaceMap mSpaceMap;
	private Document mMetadata;
	private int mReservedBlocks;


	public ManagedBlockDevice(BlockStorage aBlockStorage) throws IOException
	{
		if (aBlockStorage == null)
		{
			throw new IllegalArgumentException("aBlockStorage is null");
		}
		if (aBlockStorage.getBlockSize() < 512 || (aBlockStorage.getBlockSize() & (aBlockStorage.getBlockSize() - 1)) != 0)
		{
			throw new IllegalArgumentException("The block size must be power of 2 and at least 512 bytes in length.");
		}

		mBlockStorage = aBlockStorage;

		mMetadata = new Document();
		mReservedBlocks = 2;

		mBlockSize = mBlockStorage.getBlockSize();
		mWasCreated = mBlockStorage.size() < mReservedBlocks;

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
		log.i("create block device");
		log.inc();

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

		log.dec();
	}


	private void loadBlockDevice() throws IOException
	{
		log.i("load block device");
		log.inc();

		readSuperBlock();

		mSpaceMap = new SpaceMap(mSuperBlock, this, mBlockStorage);

		log.dec();
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
		return mBlockStorage.size() - mReservedBlocks;
	}


	@Override
	public void close() throws IOException
	{
		if (mModified)
		{
			rollback();
		}

		if (mBlockStorage != null)
		{
			mBlockStorage.resize(mSpaceMap.getRangeMap().getLastBlockIndex());
			mBlockStorage.close();
			mBlockStorage = null;
		}
	}


	public boolean isReadOnly()
	{
		return mBlockStorage.isReadOnly();
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
			throw new RaccoonIOException("Illegal block index allocated.");
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
			throw new RaccoonIOException("Illegal offset: " + aBlockIndex);
		}

		freeBlockInternal(mReservedBlocks + aBlockIndex, aBlockCount);
	}


	void freeBlockInternal(long aBlockIndex, int aBlockCount)
	{
		log.t("free block {} +{}", aBlockIndex, aBlockCount);

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
			throw new RaccoonIOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new RaccoonIOException("Illegal buffer length: " + aBlockIndex);
		}

		writeBlockInternal(mReservedBlocks + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void writeBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		mModified = true;

		log.t("write block {} +{}", aBlockIndex, aBufferLength / mBlockSize);
		log.inc();

		mBlockStorage.writeBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		log.dec();
	}


	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		if (aBlockIndex < 0)
		{
			throw new RaccoonIOException("Illegal offset: " + aBlockIndex);
		}
		if ((aBufferLength % mBlockSize) != 0)
		{
			throw new RaccoonIOException("Illegal buffer length: " + aBlockIndex);
		}

		readBlockInternal(mReservedBlocks + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	private void readBlockInternal(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assert aBufferLength > 0;
		assert (aBufferLength % mBlockSize) == 0;

		mSpaceMap.assertUsed(aBlockIndex, aBufferLength / mBlockSize);

		log.t("read block {} +{}", aBlockIndex, aBufferLength / mBlockSize);
		log.inc();

		mBlockStorage.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		log.dec();
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
			log.d("committing managed block device");
			log.inc();

			mSpaceMap.write(mSuperBlock.getSpaceMapPointer(), this, mBlockStorage);

			mBlockStorage.commit(0, false);

			writeSuperBlock();

			mBlockStorage.commit(1, aMetadata);

			mSpaceMap.reset();
			mWasCreated = false;
			mModified = false;

			log.dec();
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
			log.i("rollbacking block device");
			log.inc();

			mSpaceMap.reset();
			mSpaceMap.rollback();

			init();

			mModified = false;

			log.dec();
		}
	}


	private void readSuperBlock() throws IOException
	{
		log.d("read super block");
		log.inc();

		SuperBlock superBlockOne = new SuperBlock(-1L);
		SuperBlock superBlockTwo = new SuperBlock(-1L);

		Document metadataOne = superBlockOne.read(mBlockStorage, 0);
		Document metadataTwo = superBlockTwo.read(mBlockStorage, 1);

		if (superBlockOne.getGeneration() == superBlockTwo.getGeneration() + 1)
		{
			mSuperBlock = superBlockOne;
			mMetadata = metadataOne;

			log.t("using super block 0");
		}
		else if (superBlockTwo.getGeneration() == superBlockOne.getGeneration() + 1)
		{
			mSuperBlock = superBlockTwo;
			mMetadata = metadataTwo;

			log.t("using super block 1");
		}
		else
		{
			throw new IOException("BlockDevice appears to be corrupt. SuperBlock versions are illegal: " + superBlockOne.getGeneration() + " / " + superBlockTwo.getGeneration());
		}

		log.dec();
	}


	private void writeSuperBlock() throws IOException
	{
		int index = (int)(mSuperBlock.incrementGeneration() & 1);

		log.d("write super block {}", index);
		log.inc();

		mSuperBlock.write(mBlockStorage, index, mMetadata);

		log.dec();
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
		return mBlockStorage.size();
	}


	/**
	 * @return the number of free blocks within the allocated space.
	 */
	public long getFreeSpace()
	{
		return mBlockStorage.size() - mSpaceMap.getRangeMap().getUsedSpace();
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
		mBlockStorage.resize(mReservedBlocks + aNumberOfBlocks);
	}


	/**
	 * Frees all blocks.
	 */
	public void clear() throws IOException
	{
		mBlockStorage.resize(0);

		mSpaceMap.reset();

		createBlockDevice();
	}


	int roundUp(int aSize)
	{
		int s = mBlockStorage.getBlockSize();
		return aSize + ((s - (aSize % s)) % s);
	}
}
