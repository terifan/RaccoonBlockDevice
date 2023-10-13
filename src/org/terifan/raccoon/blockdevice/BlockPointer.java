package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.DocumentEntity;
import org.terifan.raccoon.document.Marshallable;


public final class BlockPointer extends Document //implements Marshallable
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
//	private final static int OFS_SIGNATURE = 10;

//	private final static String OFS_FLAG_TYPE = "0";
//	private final static String OFS_FLAG_LEVEL = "1";
//	private final static String OFS_FLAG_CHECKSUM = "2";
//	private final static String OFS_FLAG_COMPRESSION = "3";
//	private final static String OFS_ALLOCATED_SIZE = "4";
//	private final static String OFS_LOGICAL_SIZE = "5";
//	private final static String OFS_PHYSICAL_SIZE = "6";
//	private final static String OFS_ADDRESS = "7";
//	private final static String OFS_GENERATION = "8";
//	private final static String OFS_BLOCK_KEY = "9";
//	private final static String OFS_SIGNATURE = "10";

	private final static String OFS_FLAG_TYPE = "blk";
	private final static String OFS_FLAG_LEVEL = "lvl";
	private final static String OFS_FLAG_CHECKSUM = "chk";
	private final static String OFS_FLAG_COMPRESSION = "cmp";
	private final static String OFS_ALLOCATED_SIZE = "alo";
	private final static String OFS_LOGICAL_SIZE = "log";
	private final static String OFS_PHYSICAL_SIZE = "phy";
	private final static String OFS_ADDRESS = "adr";
	private final static String OFS_GENERATION = "gen";
	private final static String OFS_BLOCK_KEY = "key";
	private final static String OFS_SIGNATURE = "sig";

//	private Document mDocument = new Document();
	private Document mDocument = this;


	public BlockPointer()
	{
		setAddress(Array.of(-1));
	}


	public BlockPointer(Document aArray)
	{
		mDocument.putAll(aArray);
	}


	public int getBlockType()
	{
		return mDocument.get(OFS_FLAG_TYPE, BlockType.FREE);
	}


	public BlockPointer setBlockType(int aBlockType)
	{
		mDocument.put(OFS_FLAG_TYPE, aBlockType);
		return this;
	}


	public int getBlockLevel()
	{
		return mDocument.get(OFS_FLAG_LEVEL, 0);
	}


	public BlockPointer setBlockLevel(int aLevel)
	{
		mDocument.put(OFS_FLAG_LEVEL, aLevel);
		return this;
	}


	public int getChecksumAlgorithm()
	{
		return mDocument.get(OFS_FLAG_CHECKSUM, 0);
	}


	public BlockPointer setChecksumAlgorithm(int aChecksumAlgorithm)
	{
		mDocument.put(OFS_FLAG_CHECKSUM, aChecksumAlgorithm);
		return this;
	}


	public int getCompressionAlgorithm()
	{
		return mDocument.get(OFS_FLAG_COMPRESSION, 0);
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		mDocument.put(OFS_FLAG_COMPRESSION, aCompressionAlgorithm);
		return this;
	}


	public int getAllocatedSize()
	{
		return mDocument.get(OFS_ALLOCATED_SIZE, 0);
	}


	public BlockPointer setAllocatedSize(int aAllocBlocks)
	{
		assert aAllocBlocks >= 0;

		mDocument.put(OFS_ALLOCATED_SIZE, aAllocBlocks);
		return this;
	}


	public int getLogicalSize()
	{
		return mDocument.get(OFS_LOGICAL_SIZE, 0);
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		mDocument.put(OFS_LOGICAL_SIZE, aLogicalSize);
		return this;
	}


	public int getPhysicalSize()
	{
		return mDocument.get(OFS_PHYSICAL_SIZE, 0);
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		mDocument.put(OFS_PHYSICAL_SIZE, aPhysicalSize);
		return this;
	}


	public Array getBlockKey()
	{
		return mDocument.getArray(OFS_BLOCK_KEY);
	}


	public BlockPointer setBlockKey(Array aBlockKey)
	{
		mDocument.put(OFS_BLOCK_KEY, aBlockKey);
		return this;
	}


	public Array getAddress()
	{
		return mDocument.getArray(OFS_ADDRESS);
	}


	public BlockPointer setAddress(Array aAddress)
	{
		mDocument.put(OFS_ADDRESS, aAddress);
		return this;
	}


	public long getGeneration()
	{
		return mDocument.get(OFS_GENERATION, 0L);
	}


	public BlockPointer setGeneration(long aGeneration)
	{
		mDocument.put(OFS_GENERATION, aGeneration);
		return this;
	}


	public Array getChecksum()
	{
		return mDocument.getArray(OFS_SIGNATURE);
	}


	public BlockPointer setChecksum(Array aChecksum)
	{
		mDocument.put(OFS_SIGNATURE, aChecksum);
		return this;
	}


	public boolean verifyChecksum(Array aChecksum)
	{
		return aChecksum.equals(mDocument.getArray(OFS_SIGNATURE));
	}


	@Override
	public int hashCode()
	{
		return Long.hashCode(mDocument.getLong(OFS_ADDRESS));
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


//	@Override
//	public DocumentEntity marshal()
//	{
//		return mDocument;
//	}
//
//
//	@Override
//	public void unmarshal(DocumentEntity aDocumentEntity)
//	{
//		mDocument = (Document)aDocumentEntity;
//	}
}
