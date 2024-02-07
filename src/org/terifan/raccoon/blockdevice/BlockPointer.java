package org.terifan.raccoon.blockdevice;

import java.util.Arrays;
import org.terifan.logging.LogStatement;
import org.terifan.logging.LogStatementProducer;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt64;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt64;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


@LogStatementProducer
public final class BlockPointer
{
	private final Logger log = Logger.getLogger();

	public final static int SIZE = 88;

	private final static int OFS_FLAG_TYPE = 0;			// 1
	private final static int OFS_FLAG_LEVEL = 1;		// 1
	private final static int OFS_FLAG_CHECKSUM = 2;		// 1
	private final static int OFS_FLAG_COMPRESSION = 3;	// 1
	private final static int OFS_ALLOCATED_SIZE = 4;	// 4
	private final static int OFS_LOGICAL_SIZE = 8;		// 4
	private final static int OFS_PHYSICAL_SIZE = 12;	// 4
	private final static int OFS_CUMULATIVE_SIZE = 16;	// 8
	private final static int OFS_BLOCK_INDEX_0 = 24;	// 8
	private final static int OFS_BLOCK_INDEX_1 = 32;	// 8
	private final static int OFS_BLOCK_INDEX_2 = 40;	// 8
	private final static int OFS_GENERATION = 48;		// 8
	private final static int OFS_BLOCK_KEY = 56;		// 16
	private final static int OFS_CHECKSUM = 72;			// 16

	private final byte[] mBuffer;


	public BlockPointer()
	{
		mBuffer = new byte[SIZE];
		setBlockIndex0(-1);
	}


	public int getBlockType()
	{
		return mBuffer[OFS_FLAG_TYPE];
	}


	public BlockPointer setBlockType(int aBlockType)
	{
		mBuffer[OFS_FLAG_TYPE] = (byte)aBlockType;
		return this;
	}


	public int getBlockLevel()
	{
		return mBuffer[OFS_FLAG_LEVEL];
	}


	public BlockPointer setBlockLevel(int aLevel)
	{
		mBuffer[OFS_FLAG_LEVEL] = (byte)aLevel;
		return this;
	}


	public int getChecksumAlgorithm()
	{
		return mBuffer[OFS_FLAG_CHECKSUM];
	}


	public BlockPointer setChecksumAlgorithm(int aChecksumAlgorithm)
	{
		mBuffer[OFS_FLAG_CHECKSUM] = (byte)aChecksumAlgorithm;
		return this;
	}


	public int getCompressionAlgorithm()
	{
		return mBuffer[OFS_FLAG_COMPRESSION];
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		mBuffer[OFS_FLAG_COMPRESSION] = (byte)aCompressionAlgorithm;
		return this;
	}


	public int getAllocatedSize()
	{
		return getInt32(mBuffer, OFS_ALLOCATED_SIZE);
	}


	public BlockPointer setAllocatedSize(int aAllocBlocks)
	{
		assert aAllocBlocks >= 0;

		putInt32(mBuffer, OFS_ALLOCATED_SIZE, aAllocBlocks);
		return this;
	}


	public int getLogicalSize()
	{
		return getInt32(mBuffer, OFS_LOGICAL_SIZE);
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		putInt32(mBuffer, OFS_LOGICAL_SIZE, aLogicalSize);
		return this;
	}


	public int getPhysicalSize()
	{
		return getInt32(mBuffer, OFS_PHYSICAL_SIZE);
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		putInt32(mBuffer, OFS_PHYSICAL_SIZE, aPhysicalSize);
		return this;
	}


	public long getCumulativeSize()
	{
		return getInt64(mBuffer, OFS_CUMULATIVE_SIZE);
	}


	public BlockPointer setCumulativeSize(long aCumulativeSize)
	{
		putInt64(mBuffer, OFS_CUMULATIVE_SIZE, aCumulativeSize);
		return this;
	}


	public int[] getBlockKey()
	{
		return new int[]
		{
			getInt32(mBuffer, OFS_BLOCK_KEY),
			getInt32(mBuffer, OFS_BLOCK_KEY + 4),
			getInt32(mBuffer, OFS_BLOCK_KEY + 8),
			getInt32(mBuffer, OFS_BLOCK_KEY + 12)
		};
	}


	public BlockPointer setBlockKey(int[] aBlockKey)
	{
		putInt32(mBuffer, OFS_BLOCK_KEY, aBlockKey[0]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 4, aBlockKey[1]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 8, aBlockKey[2]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 12, aBlockKey[3]);
		return this;
	}


	public long getBlockIndex0()
	{
		return getInt64(mBuffer, OFS_BLOCK_INDEX_0);
	}


	public BlockPointer setBlockIndex0(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_BLOCK_INDEX_0, aBlockIndex);
		return this;
	}


	public long getBlockIndex1()
	{
		return getInt64(mBuffer, OFS_BLOCK_INDEX_1);
	}


	public BlockPointer setBlockIndex1(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_BLOCK_INDEX_1, aBlockIndex);
		return this;
	}


	public long getBlockIndex2()
	{
		return getInt64(mBuffer, OFS_BLOCK_INDEX_2);
	}


	public BlockPointer setBlockIndex2(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_BLOCK_INDEX_2, aBlockIndex);
		return this;
	}


	public long getGeneration()
	{
		return getInt64(mBuffer, OFS_GENERATION);
	}


	public BlockPointer setGeneration(long aGeneration)
	{
		putInt64(mBuffer, OFS_GENERATION, aGeneration);
		return this;
	}


	public int[] getChecksum()
	{
		return new int[]
		{
			getInt32(mBuffer, OFS_CHECKSUM),
			getInt32(mBuffer, OFS_CHECKSUM + 4),
			getInt32(mBuffer, OFS_CHECKSUM + 8),
			getInt32(mBuffer, OFS_CHECKSUM + 12)
		};
	}


	public BlockPointer setChecksum(int[] aChecksum)
	{
		putInt32(mBuffer, OFS_CHECKSUM, aChecksum[0]);
		putInt32(mBuffer, OFS_CHECKSUM + 4, aChecksum[1]);
		putInt32(mBuffer, OFS_CHECKSUM + 8, aChecksum[2]);
		putInt32(mBuffer, OFS_CHECKSUM + 12, aChecksum[3]);
		return this;
	}


	@Override
	public int hashCode()
	{
		return Arrays.hashCode(mBuffer);
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer == this)
		{
			return true;
		}
		if (aBlockPointer instanceof BlockPointer v)
		{
			return Arrays.equals(mBuffer, v.mBuffer);
		}
		return false;
	}


	@Override
	public String toString()
	{
		return generateLog().toString();
	}


	@LogStatementProducer
	private LogStatement generateLog()
	{
		return new LogStatement("type={}, level={}, index={}, alloc={}, phys={}, logic={}, cum={}, gen={}, cmp={}, chk={}", BlockType.lookup(getBlockType()), getBlockLevel(), getBlockIndex0(), getAllocatedSize(), getPhysicalSize(), getLogicalSize(), getCumulativeSize(), getGeneration(), getCompressionAlgorithm(), 0xffffffffL & getChecksum()[0]).setType("BlockPointer");
	}


	public byte[] marshal()
	{
		return mBuffer;
	}


	public BlockPointer unmarshal(byte[] aDocument)
	{
		System.arraycopy(aDocument, 0, mBuffer, 0, SIZE);
		return this;
	}


	public BlockPointer unmarshalBuffer(ByteArrayBuffer aDocument)
	{
		aDocument.read(mBuffer);
		return this;
	}


	public Document marshalDocument()
	{
		Array pointers = Array.of(getBlockIndex0(), getBlockIndex1(), getBlockIndex2());
		if (pointers.getLast().equals(0L)) pointers.removeLast();
		if (pointers.getLast().equals(0L)) pointers.removeLast();

		return new Document()
			.put("0", getBlockType())
			.put("1", getBlockLevel())
			.put("2", getChecksumAlgorithm())
			.put("3", getCompressionAlgorithm())
			.put("4", getAllocatedSize())
			.put("5", getLogicalSize())
			.put("6", getPhysicalSize())
			.put("7", getCumulativeSize())
			.put("8", pointers)
			.put("9", getGeneration())
			.put("10", Array.of(getBlockKey()))
			.put("11", Array.of(getChecksum()));
	}


	public BlockPointer unmarshalDocument(Document aDocument)
	{
		setBlockType(aDocument.getInt("0"));
		setBlockLevel(aDocument.getInt("1"));
		setChecksumAlgorithm(aDocument.getInt("2"));
		setCompressionAlgorithm(aDocument.getInt("3"));
		setAllocatedSize(aDocument.getInt("4"));
		setLogicalSize(aDocument.getInt("5"));
		setPhysicalSize(aDocument.getInt("6"));
		setCumulativeSize(aDocument.getLong("7"));
		setBlockIndex0(aDocument.getArray("8").get(0, 0L));
		setBlockIndex1(aDocument.getArray("8").get(1, 0L));
		setBlockIndex2(aDocument.getArray("8").get(2, 0L));
		setGeneration(aDocument.getInt("9"));
		setBlockKey(aDocument.getArray("10").asInts());
		setChecksum(aDocument.getArray("11").asInts());
		return this;
	}
}
