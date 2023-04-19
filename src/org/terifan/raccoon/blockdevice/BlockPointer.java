package org.terifan.raccoon.blockdevice;

import java.io.Serializable;
import java.util.Arrays;
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
	public final static int SIZE = 96;

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
	private final static int OFS_TRANSACTION = 40;		// 8	40..47
	private final static int OFS_BLOCK_KEY = 48;		// 16	48..63
	private final static int OFS_CHECKSUM = 64;			// 32	64..95

	private byte[] mBuffer;


	public BlockPointer()
	{
		mBuffer = new byte[SIZE];
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


	/**
	 * Return the 'type' field from a BlockPointer stored in the buffer provided.
	 *
	 * @param aBuffer a buffer containing a BlockPointer
	 * @param aBlockPointerOffset start offset of the BlockPointer in the buffer
	 * @return the 'type' field
	 */
	public static int readBlockType(byte[] aBuffer, int aBlockPointerOffset)
	{
		return 0xFF & aBuffer[aBlockPointerOffset + OFS_FLAG_TYPE];
	}


	public byte getChecksumAlgorithm()
	{
		return mBuffer[OFS_FLAG_CHECKSUM];
	}


	public BlockPointer setChecksumAlgorithm(byte aChecksumAlgorithm)
	{
		mBuffer[OFS_FLAG_CHECKSUM] = aChecksumAlgorithm;
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
		int[] blockKey = new int[4];
		blockKey[0] = getInt32(mBuffer, OFS_BLOCK_KEY + 0);
		blockKey[1] = getInt32(mBuffer, OFS_BLOCK_KEY + 4);
		blockKey[2] = getInt32(mBuffer, OFS_BLOCK_KEY + 8);
		blockKey[3] = getInt32(mBuffer, OFS_BLOCK_KEY + 12);
		return blockKey;
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


	public long getTransactionId()
	{
		return getInt64(mBuffer, OFS_TRANSACTION);
	}


	public BlockPointer setTransactionId(long aTransactionId)
	{
		putInt64(mBuffer, OFS_TRANSACTION, aTransactionId);
		return this;
	}


	public long[] getChecksum()
	{
		long[] aChecksum = new long[4];
		aChecksum[0] = getInt64(mBuffer, OFS_CHECKSUM + 0);
		aChecksum[1] = getInt64(mBuffer, OFS_CHECKSUM + 8);
		aChecksum[2] = getInt64(mBuffer, OFS_CHECKSUM + 16);
		aChecksum[3] = getInt64(mBuffer, OFS_CHECKSUM + 24);
		return aChecksum;
	}


	public BlockPointer setChecksum(long... aChecksum)
	{
		assert aChecksum.length == 4;

		putInt64(mBuffer, OFS_CHECKSUM + 0, aChecksum[0]);
		putInt64(mBuffer, OFS_CHECKSUM + 8, aChecksum[1]);
		putInt64(mBuffer, OFS_CHECKSUM + 16, aChecksum[2]);
		putInt64(mBuffer, OFS_CHECKSUM + 24, aChecksum[3]);
		return this;
	}


	public boolean verifyChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		return aChecksum[0] == getInt64(mBuffer, OFS_CHECKSUM + 0)
			&& aChecksum[1] == getInt64(mBuffer, OFS_CHECKSUM + 8)
			&& aChecksum[2] == getInt64(mBuffer, OFS_CHECKSUM + 16)
			&& aChecksum[3] == getInt64(mBuffer, OFS_CHECKSUM + 24);
	}


	public byte[] marshal()
	{
		return marshal(ByteArrayBuffer.alloc(SIZE)).array();
	}


	public BlockPointer unmarshal(byte[] aBinary)
	{
		return unmarshal(ByteArrayBuffer.wrap(aBinary));
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		return aBuffer.write(mBuffer);
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.read(mBuffer);
		return this;
	}


	public BlockPointer unmarshalDoc(Array aArray)
	{
		setBlockType(aArray.getInt(0));
		setBlockLevel(aArray.getInt(1));
		setCompressionAlgorithm(aArray.getInt(2));
		setAllocatedSize(aArray.getInt(3));
		setLogicalSize(aArray.getInt(4));
		setPhysicalSize(aArray.getInt(5));
		setTransactionId(aArray.getInt(6));
		setBlockIndex0(aArray.getArray(7).getLong(0));
		setBlockIndex1(aArray.getArray(7).getLong(1));
		setBlockIndex2(aArray.getArray(7).getLong(2));
		setBlockKey(aArray.getArray(8).toInts());
		setChecksum(aArray.getArray(9).toLongs());
		return this;
	}


	public Array marshalDoc()
	{
		return Array.of(
			getBlockType(),
			getBlockLevel(),
			getCompressionAlgorithm(),
			getAllocatedSize(),
			getLogicalSize(),
			getPhysicalSize(),
			Array.of(getBlockIndex0(), getBlockIndex1(), getBlockIndex2()),
			Array.of(getBlockKey()),
			Array.of(getChecksum()),
			getTransactionId()
		);
	}


	public BlockPointer unmarshalDoc2(Document aDocument)
	{
		setBlockType(aDocument.getInt("t"));
		setBlockLevel(aDocument.getInt("l"));
		setCompressionAlgorithm(aDocument.getInt("c"));
		setAllocatedSize(aDocument.getInt("a"));
		setLogicalSize(aDocument.getInt("s"));
		setPhysicalSize(aDocument.getInt("p"));
		setTransactionId(aDocument.getInt("x"));
		setBlockIndex0(aDocument.getArray("b").getLong(0));
		setBlockKey(aDocument.getArray("k").toInts());
		setChecksum(aDocument.getArray("d").toLongs());
		return this;
	}


	public Document marshalDoc2()
	{
		Document doc = new Document()
			.put("typ", getBlockType())
			.put("lvl", getBlockLevel())
			.put("cmp", getCompressionAlgorithm())
			.put("siz", Array.of(getAllocatedSize(), getLogicalSize(), getPhysicalSize()))
			.put("blk", Array.of(getBlockIndex0(), getBlockIndex1(), getBlockIndex2()))
			.put("key", Array.of(getBlockKey()))
			.put("chk", Array.of(getChecksum()))
			.put("tx", getTransactionId())
			;

		return doc;
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
		return Console.format("{type=%d, offset=%d, alloc=%d, phys=%d, logic=%d, tx=%d}", getBlockType(), getBlockIndex0(), getAllocatedSize(), getPhysicalSize(), getLogicalSize(), getTransactionId());
	}
}
