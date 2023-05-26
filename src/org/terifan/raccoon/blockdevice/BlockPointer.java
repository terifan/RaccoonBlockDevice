package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


public final class BlockPointer extends Document
{
	private final static long serialVersionUID = 1;

//	private final static int OFS_FLAG_TYPE = 0;
//	private final static int OFS_FLAG_LEVEL = 1;
//	private final static int OFS_FLAG_CHECKSUM = 2;
//	private final static int OFS_FLAG_COMPRESSION = 3;
//	private final static int OFS_ALLOCATED_SIZE = 4;
//	private final static int OFS_LOGICAL_SIZE = 5;
//	private final static int OFS_PHYSICAL_SIZE = 6;
//	private final static int OFS_ADDRESS = 7;
//	private final static int OFS_GENERATION = 8;
//	private final static int OFS_BLOCK_KEY = 9;
//	private final static int OFS_CHECKSUM = 10;

	private final static String OFS_FLAG_TYPE = "0";
	private final static String OFS_FLAG_LEVEL = "1";
	private final static String OFS_FLAG_CHECKSUM = "2";
	private final static String OFS_FLAG_COMPRESSION = "3";
	private final static String OFS_ALLOCATED_SIZE = "4";
	private final static String OFS_LOGICAL_SIZE = "5";
	private final static String OFS_PHYSICAL_SIZE = "6";
	private final static String OFS_ADDRESS = "7";
	private final static String OFS_GENERATION = "8";
	private final static String OFS_BLOCK_KEY = "9";
	private final static String OFS_CHECKSUM = "10";

//	private final static String OFS_FLAG_TYPE = "type";
//	private final static String OFS_FLAG_LEVEL = "level";
//	private final static String OFS_FLAG_CHECKSUM = "checksum_type";
//	private final static String OFS_FLAG_COMPRESSION = "compressor_type";
//	private final static String OFS_ALLOCATED_SIZE = "allocated";
//	private final static String OFS_LOGICAL_SIZE = "logical";
//	private final static String OFS_PHYSICAL_SIZE = "physical";
//	private final static String OFS_ADDRESS = "address";
//	private final static String OFS_GENERATION = "generation";
//	private final static String OFS_BLOCK_KEY = "block_key";
//	private final static String OFS_CHECKSUM = "checksum";


	public BlockPointer()
	{
		setAddress(Array.of(-1));
	}


	public BlockPointer(Document aArray)
	{
		putAll(aArray);
	}


	public int getBlockType()
	{
		return get(OFS_FLAG_TYPE, BlockType.FREE);
	}


	public BlockPointer setBlockType(int aBlockType)
	{
		put(OFS_FLAG_TYPE, aBlockType);
		return this;
	}


	public int getBlockLevel()
	{
		return get(OFS_FLAG_LEVEL, 0);
	}


	public BlockPointer setBlockLevel(int aLevel)
	{
		put(OFS_FLAG_LEVEL, aLevel);
		return this;
	}


	public int getChecksumAlgorithm()
	{
		return get(OFS_FLAG_CHECKSUM, 0);
	}


	public BlockPointer setChecksumAlgorithm(int aChecksumAlgorithm)
	{
		put(OFS_FLAG_CHECKSUM, aChecksumAlgorithm);
		return this;
	}


	public int getCompressionAlgorithm()
	{
		return get(OFS_FLAG_COMPRESSION, 0);
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		put(OFS_FLAG_COMPRESSION, aCompressionAlgorithm);
		return this;
	}


	public int getAllocatedSize()
	{
		return get(OFS_ALLOCATED_SIZE, 0);
	}


	public BlockPointer setAllocatedSize(int aAllocBlocks)
	{
		assert aAllocBlocks >= 0;

		put(OFS_ALLOCATED_SIZE, aAllocBlocks);
		return this;
	}


	public int getLogicalSize()
	{
		return get(OFS_LOGICAL_SIZE, 0);
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		put(OFS_LOGICAL_SIZE, aLogicalSize);
		return this;
	}


	public int getPhysicalSize()
	{
		return get(OFS_PHYSICAL_SIZE, 0);
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		put(OFS_PHYSICAL_SIZE, aPhysicalSize);
		return this;
	}


	public Array getBlockKey()
	{
		return getArray(OFS_BLOCK_KEY);
	}


	public BlockPointer setBlockKey(Array aBlockKey)
	{
		put(OFS_BLOCK_KEY, aBlockKey);
		return this;
	}


	public Array getAddress()
	{
		return getArray(OFS_ADDRESS);
	}


	public BlockPointer setAddress(Array aAddress)
	{
		put(OFS_ADDRESS, aAddress);
		return this;
	}


	public long getGeneration()
	{
		return get(OFS_GENERATION, 0L);
	}


	public BlockPointer setGeneration(long aGeneration)
	{
		put(OFS_GENERATION, aGeneration);
		return this;
	}


	public Array getChecksum()
	{
		return getArray(OFS_CHECKSUM);
	}


	public BlockPointer setChecksum(Array aChecksum)
	{
		put(OFS_CHECKSUM, aChecksum);
		return this;
	}


	public boolean verifyChecksum(Array aChecksum)
	{
		return aChecksum.equals(getArray(OFS_CHECKSUM));
	}


	@Override
	public int hashCode()
	{
		return Long.hashCode(getLong(OFS_ADDRESS));
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			BlockPointer other = (BlockPointer)aBlockPointer;
			return other.getAddress().get(0).equals(getAddress().get(0));
		}
		return false;
	}


	@Override
	public String toString()
	{
		return Console.format("{type=%d, level=%d, address=%s, alloc=%d, phys=%d, logic=%d, tx=%d}", getBlockType(), getBlockLevel(), getAddress(), getAllocatedSize(), getPhysicalSize(), getLogicalSize(), getGeneration());
	}


	public long getBlockIndex0()
	{
		return getAddress().get(0, -1L);
	}
}
