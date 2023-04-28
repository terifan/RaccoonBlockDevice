package org.terifan.raccoon.blockdevice;

import java.io.Serializable;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt64;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt64;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.document.Document;


public final class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 80;

	private final static int OFS_FLAG_TYPE = 0;			// 1	0
	private final static int OFS_FLAG_LEVEL = 1;		// 1	1
	private final static int OFS_FLAG_CHECKSUM = 2;		// 1	2
	private final static int OFS_FLAG_COMPRESSION = 3;	// 1	3
	private final static int OFS_ALLOCATED_SIZE = 4;	// 4	4..7
	private final static int OFS_LOGICAL_SIZE = 8;		// 4	8..11
	private final static int OFS_PHYSICAL_SIZE = 12;	// 4	12..15
	private final static int OFS_OFFSET0 = 16;			// 8	16..23
	private final static int OFS_OFFSET1 = 24;			// 8	24..31
	private final static int OFS_OFFSET2 = 32;			// 8	32..40
	private final static int OFS_GENERATION = 40;		// 8	40..47
	private final static int OFS_BLOCK_KEY = 48;		// 16	48..63
	private final static int OFS_CHECKSUM = 64;			// 16	64..79

	private byte[] mBuffer;


	public BlockPointer()
	{
		mBuffer = new byte[SIZE];
		setBlockIndex0(-1);
		setBlockIndex1(-1);
		setBlockIndex2(-1);
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
		return 0xff & mBuffer[OFS_FLAG_LEVEL];
	}


	public BlockPointer setBlockLevel(int aLevel)
	{
		mBuffer[OFS_FLAG_LEVEL] = (byte)aLevel;
		return this;
	}


	public byte getChecksumAlgorithm()
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
		return 0xff & mBuffer[OFS_FLAG_COMPRESSION];
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		assert aCompressionAlgorithm >= 0 && aCompressionAlgorithm <= 255;
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


	public int[] getBlockKey()
	{
		return new int[]
		{
			getInt32(mBuffer, OFS_BLOCK_KEY + 0),
			getInt32(mBuffer, OFS_BLOCK_KEY + 4),
			getInt32(mBuffer, OFS_BLOCK_KEY + 8),
			getInt32(mBuffer, OFS_BLOCK_KEY + 12)
		};
	}


	public BlockPointer setBlockKey(int... aBlockKey)
	{
		assert aBlockKey.length == 4 : aBlockKey.length;

		putInt32(mBuffer, OFS_BLOCK_KEY + 0, aBlockKey[0]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 4, aBlockKey[1]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 8, aBlockKey[2]);
		putInt32(mBuffer, OFS_BLOCK_KEY + 12, aBlockKey[3]);
		return this;
	}


	public long getBlockIndex0()
	{
		return getInt64(mBuffer, OFS_OFFSET0);
	}


	public BlockPointer setBlockIndex0(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_OFFSET0, aBlockIndex);
		return this;
	}


	public long getBlockIndex1()
	{
		return getInt64(mBuffer, OFS_OFFSET1);
	}


	public BlockPointer setBlockIndex1(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_OFFSET1, aBlockIndex);
		return this;
	}


	public long getBlockIndex2()
	{
		return getInt64(mBuffer, OFS_OFFSET2);
	}


	public BlockPointer setBlockIndex2(long aBlockIndex)
	{
		putInt64(mBuffer, OFS_OFFSET2, aBlockIndex);
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
			getInt32(mBuffer, OFS_CHECKSUM + 0),
			getInt32(mBuffer, OFS_CHECKSUM + 4),
			getInt32(mBuffer, OFS_CHECKSUM + 8),
			getInt32(mBuffer, OFS_CHECKSUM + 12)
		};
	}


	public BlockPointer setChecksum(int... aChecksum)
	{
		assert aChecksum.length == 4;

		putInt32(mBuffer, OFS_CHECKSUM + 0, aChecksum[0]);
		putInt32(mBuffer, OFS_CHECKSUM + 4, aChecksum[1]);
		putInt32(mBuffer, OFS_CHECKSUM + 8, aChecksum[2]);
		putInt32(mBuffer, OFS_CHECKSUM + 12, aChecksum[3]);
		return this;
	}


	public boolean verifyChecksum(int[] aChecksum)
	{
		assert aChecksum.length == 4;

		return aChecksum[0] == getInt32(mBuffer, OFS_CHECKSUM + 0)
			&& aChecksum[1] == getInt32(mBuffer, OFS_CHECKSUM + 4)
			&& aChecksum[2] == getInt32(mBuffer, OFS_CHECKSUM + 8)
			&& aChecksum[3] == getInt32(mBuffer, OFS_CHECKSUM + 12);
	}


	public byte[] marshal()
	{
		return marshal(ByteArrayBuffer.alloc(SIZE)).array();
	}


	public BlockPointer unmarshal(byte[] aBinary)
	{
		return unmarshal(ByteArrayBuffer.wrap(aBinary));
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aDestination)
	{
		return aDestination.write(mBuffer);
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.read(mBuffer);
		return this;
	}


	public BlockPointer unmarshalDoc(Document aDocument)
	{
		Array array = aDocument.getArray("inf");
		setBlockType(array.getInt(0));
		setBlockLevel(array.getInt(1));
		setChecksumAlgorithm(array.getByte(2));
		setCompressionAlgorithm(array.getInt(3));
		setAllocatedSize(array.getInt(4));
		setLogicalSize(array.getInt(5));
		setPhysicalSize(array.getInt(6));
		setGeneration(array.getLong(7));

		Array adr = aDocument.getArray("adr");
		if(adr.size()>0)setBlockIndex0(adr.getLong(0));
		if(adr.size()>1)setBlockIndex1(adr.getLong(1));
		if(adr.size()>2)setBlockIndex2(adr.getLong(2));

		setBlockKey(aDocument.getArray("key").toInts());
		setChecksum(aDocument.getArray("chk").toInts());
		return this;
	}


	public Document marshalDoc()
	{
		Array adr = new Array();
		if (getBlockIndex0() >= 0) adr.add(getBlockIndex0());
		if (getBlockIndex1() >= 0) adr.add(getBlockIndex1());
		if (getBlockIndex2() >= 0) adr.add(getBlockIndex2());

		return new Document()
			.put("inf", Array.of(
				getBlockType(),
				getBlockLevel(),
				getChecksumAlgorithm(),
				getCompressionAlgorithm(),
				getAllocatedSize(),
				getLogicalSize(),
				getPhysicalSize(),
				getGeneration())
			)
			.put("adr", adr)
			.put("key", Array.of(getBlockKey()))
			.put("chk", Array.of(getChecksum()));
	}


	public BlockPointer unmarshalDoc2(Array aArray)
	{
		setBlockType(aArray.getInt(0));
		setBlockLevel(aArray.getInt(1));
		setCompressionAlgorithm(aArray.getInt(2));
		setChecksumAlgorithm(aArray.getByte(3));
		setAllocatedSize(aArray.getInt(4));
		setLogicalSize(aArray.getInt(5));
		setPhysicalSize(aArray.getInt(6));
		setGeneration(aArray.getLong(7));
		Array ptr = aArray.getArray(8);
		if(ptr.size()>0)setBlockIndex0(ptr.getLong(0));
		if(ptr.size()>1)setBlockIndex1(ptr.getLong(1));
		if(ptr.size()>2)setBlockIndex2(ptr.getLong(2));
		setBlockKey(aArray.getArray(9).toInts());
		setChecksum(aArray.getArray(10).toInts());
		return this;
	}


	public Array marshalDoc2()
	{
		Array ptr = new Array();
		if (getBlockIndex0() >= 0) ptr.add(getBlockIndex0());
		if (getBlockIndex1() >= 0) ptr.add(getBlockIndex1());
		if (getBlockIndex2() >= 0) ptr.add(getBlockIndex2());

		return Array.of(getBlockType(),
			getBlockLevel(),
			getCompressionAlgorithm(),
			getChecksumAlgorithm(),
			getAllocatedSize(),
			getLogicalSize(),
			getPhysicalSize(),
			getGeneration(), ptr,
			Array.of(getBlockKey()),
			Array.of(getChecksum())
		);
	}


	@Override
	public int hashCode()
	{
		return Long.hashCode(getBlockIndex0());
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getBlockIndex0() == getBlockIndex0();
		}
		return false;
	}


	@Override
	public String toString()
	{
		return Console.format("{type=%d, level=%d, offset=%d, alloc=%d, phys=%d, logic=%d, tx=%d}", getBlockType(), getBlockLevel(), getBlockIndex0(), getAllocatedSize(), getPhysicalSize(), getLogicalSize(), getGeneration());
	}
}
