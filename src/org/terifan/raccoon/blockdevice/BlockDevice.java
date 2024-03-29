package org.terifan.raccoon.blockdevice;


public interface BlockDevice extends AutoCloseable
{
	boolean isReadOnly();


	/**
	 * Read a single block from the device.
	 *
	 * @param aBlockIndex the index of the block.
	 * @param aBuffer destination array for block the be read
	 * @param aBufferOffset offset in the block array where block data is stored
	 * @param aBufferLength length of buffer to write, must be dividable by block size
	 * @param aBlockKey 128 bit seed value that may be used by block device implementations performing cryptography
	 */
	void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey);


	/**
	 * Write a single block to the device.
	 *
	 * @param aBlockIndex the index of the block.
	 * @param aBuffer data to be written to the device
	 * @param aBufferOffset offset in the block array where block data is stored
	 * @param aBufferLength length of buffer to write, must be dividable by block size
	 * @param aBlockKey 128 bit seed value that may be used by block device implementations performing cryptography
	 */
	void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey);


	/**
	 * Attempt to flush any changes made to blocks
	 *
	 * @param aMetadata force update of metadata
	 */
	void commit(int aIndex, boolean aMetadata);


	/**
	 * Close the block device. Information not previously committed will be lost.
	 */
	@Override
	void close();


	/**
	 * @return number of blocks available on this device.
	 */
	long size();


	/**
	 * @return the size of each block on this device.
	 */
	int getBlockSize();


	/**
	 * Truncates or expands this block device to the number of blocks specified.
	 *
	 * @param aNumberOfBlocks number of blocks
	 */
	void resize(long aNumberOfBlocks);
}
