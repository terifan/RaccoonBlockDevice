package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.compressor.ByteBlockOutputStream;
import org.terifan.raccoon.blockdevice.compressor.Compressor;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.secure.BlockKeyGenerator;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


public class BlockAccessor implements AutoCloseable
{
	private final ManagedBlockDevice mBlockDevice;
	private boolean mCloseUnderlyingDevice;


	public BlockAccessor(ManagedBlockDevice aBlockDevice, boolean aCloseUnderlyingDevice)
	{
		mBlockDevice = aBlockDevice;
		mCloseUnderlyingDevice = aCloseUnderlyingDevice;
	}


	public ManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	@Override
	public void close()
	{
		if (mCloseUnderlyingDevice)
		{
			mBlockDevice.close();
		}
	}


	public synchronized void freeBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("free block %s", aBlockPointer);
			Log.inc();

			mBlockDevice.freeBlock(aBlockPointer.getBlockIndex0(), aBlockPointer.getAllocatedSize() / mBlockDevice.getBlockSize());

//			assert collectStatistics(FREE_BLOCK, aBlockPointer.getAllocatedSize());

			Log.dec();
		}
		catch (Exception | Error e)
		{
			throw new RaccoonIOException(aBlockPointer.toString(), e);
		}
	}


	public synchronized byte[] readBlock(BlockPointer aBlockPointer)
	{
		try
		{
			Log.d("read block %s", aBlockPointer);
			Log.inc();

			byte[] buffer = new byte[aBlockPointer.getAllocatedSize()];

			mBlockDevice.readBlock(aBlockPointer.getBlockIndex0(), buffer, 0, buffer.length, aBlockPointer.getBlockKey(new int[8]));

			long[] hash = MurmurHash3.hash256(buffer, 0, aBlockPointer.getPhysicalSize(), aBlockPointer.getTransactionId());

			if (!aBlockPointer.verifyChecksum(hash))
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			Compressor compressor = CompressorLevel.values()[aBlockPointer.getCompressionAlgorithm()].instance();

			if (compressor != null)
			{
				byte[] tmp = new byte[aBlockPointer.getLogicalSize()];
				compressor.decompress(buffer, 0, aBlockPointer.getPhysicalSize(), tmp, 0, tmp.length);
				buffer = tmp;
			}
			else if (aBlockPointer.getLogicalSize() < buffer.length)
			{
				buffer = Arrays.copyOfRange(buffer, 0, aBlockPointer.getLogicalSize());
			}

//			assert collectStatistics(READ_BLOCK, buffer.length);

			Log.dec();

			return buffer;
		}
		catch (Exception | Error e)
		{
			throw new RaccoonIOException("Error reading block: " + aBlockPointer, e);
		}
	}


	public synchronized BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, int aBlockType, CompressorLevel aCompressorLevel)
	{
		long transactionId = getBlockDevice().getTransactionId();
		BlockPointer blockPointer = null;

		try
		{
			ByteBlockOutputStream compressedBlock = null;
			boolean compressed = false;

			if (aCompressorLevel != CompressorLevel.NONE)
			{
				compressedBlock = new ByteBlockOutputStream(mBlockDevice.getBlockSize());
				compressed = aCompressorLevel.instance().compress(aBuffer, aOffset, aLength, compressedBlock);
			}

			int physicalSize;
			if (compressed && roundUp(compressedBlock.size()) < roundUp(aBuffer.length)) // use the compressed result only if we actual save one block or more
			{
				physicalSize = compressedBlock.size();
				aBuffer = compressedBlock.getBuffer();
			}
			else
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + roundUp(aLength));
				aCompressorLevel = CompressorLevel.NONE;
			}

			assert aBuffer.length % mBlockDevice.getBlockSize() == 0;

			long blockIndex = mBlockDevice.allocBlock(aBuffer.length / mBlockDevice.getBlockSize());
			int[] blockKey = BlockKeyGenerator.generate();

			blockPointer = new BlockPointer()
				.setCompressionAlgorithm(aCompressorLevel.ordinal())
				.setAllocatedSize(aBuffer.length)
				.setPhysicalSize(physicalSize)
				.setLogicalSize(aLength)
				.setTransactionId(transactionId)
				.setBlockType(aBlockType)
				.setChecksumAlgorithm((byte)0)
				.setChecksum(MurmurHash3.hash256(aBuffer, 0, physicalSize, transactionId))
				.setBlockKey(blockKey)
				.setBlockIndex0(blockIndex);

			Log.d("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockKey);

//			assert collectStatistics(WRITE_BLOCK, aBuffer.length);

			Log.dec();

			return blockPointer;
		}
		catch (Exception | Error e)
		{
			throw new RaccoonIOException("Error writing block: " + blockPointer, e);
		}
	}


	private int roundUp(int aSize)
	{
		int s = mBlockDevice.getBlockSize();
		int d = aSize % s;
		if (d == 0)
		{
			return aSize;
		}
		return aSize + (s - d);
	}
}
