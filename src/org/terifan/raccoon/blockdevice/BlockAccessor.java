package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.compressor.ByteBlockOutputStream;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.security.random.ISAAC;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;


public class BlockAccessor implements AutoCloseable
{
	private final static ISAAC PRNG = new ISAAC();
	private final ManagedBlockDevice mBlockDevice;
	private final boolean mCloseUnderlyingDevice;

	private int mChecksumAlgorithm = ChecksumAlgorithm.MurmurHash3;


	public BlockAccessor(ManagedBlockDevice aBlockDevice)
	{
		mBlockDevice = aBlockDevice;
		mCloseUnderlyingDevice = false;
	}


	public BlockAccessor(ManagedBlockDevice aBlockDevice, boolean aCloseUnderlyingDevice)
	{
		mBlockDevice = aBlockDevice;
		mCloseUnderlyingDevice = aCloseUnderlyingDevice;
	}


	public BlockAccessor setChecksumAlgorithm(int aChecksumAlgorithm)
	{
		mChecksumAlgorithm = aChecksumAlgorithm;
		return this;
	}


	public ManagedBlockDevice getBlockDevice()
	{
		return mBlockDevice;
	}


	@Override
	public void close() throws IOException
	{
		if (mCloseUnderlyingDevice)
		{
			mBlockDevice.close();
		}
	}


	public synchronized void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer == null || aBlockPointer.getPhysicalSize() == 0)
		{
			return;
		}

		try
		{
			Log.d("free block %S", aBlockPointer);
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
		if (aBlockPointer.getPhysicalSize() == 0)
		{
			return new byte[aBlockPointer.getLogicalSize()];
		}

		return readBlock(aBlockPointer, new byte[aBlockPointer.getLogicalSize()]);
	}


	public synchronized byte[] readBlock(BlockPointer aBlockPointer, final byte[] aBuffer)
	{
		Log.d("read block  %S", aBlockPointer);

		if (aBlockPointer.getPhysicalSize() > aBuffer.length)
		{
			throw new IllegalArgumentException();
		}

		if (aBlockPointer.getPhysicalSize() == 0)
		{
			return new byte[aBlockPointer.getLogicalSize()];
		}

		Log.inc();

		mBlockDevice.readBlock(aBlockPointer.getBlockIndex0(), aBuffer, 0, aBlockPointer.getAllocatedSize(), aBlockPointer.getBlockKey());

		if (!CompressorAlgorithm.decompress(aBlockPointer.getCompressionAlgorithm(), aBuffer, aBlockPointer.getPhysicalSize(), aBlockPointer.getLogicalSize()))
		{
			throw new RaccoonIOException("Error decompressing data.");
		}

		assert isAllZeros(aBuffer, aBlockPointer.getLogicalSize(), aBuffer.length - aBlockPointer.getLogicalSize());

		int[] checksum = ChecksumAlgorithm.hash128(aBlockPointer.getChecksumAlgorithm(), aBuffer, 0, aBlockPointer.getLogicalSize(), aBlockPointer.getGeneration());

		if (!Arrays.equals(aBlockPointer.getChecksum(), checksum))
		{
			throw new RaccoonIOException("Checksum error in block " + aBlockPointer);
		}

//		assert collectStatistics(READ_BLOCK, buffer.length);
		Log.dec();

		return aBuffer;
	}


	public synchronized BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, int aBlockType, int aBlockLevel, int aCompressorLevel)
	{
		if (isAllZeros(aBuffer, aOffset, aLength))
		{
			BlockPointer blockPointer = new BlockPointer()
				.setBlockType(aBlockType)
				.setBlockLevel(aBlockLevel)
				.setLogicalSize(aLength)
				.setBlockIndex0(0)
				.setGeneration(mBlockDevice.getGeneration());

			Log.d("write block %S", blockPointer);

			return blockPointer;
		}

		BlockPointer blockPointer = null;

		try
		{
			int[] checksum = ChecksumAlgorithm.hash128(mChecksumAlgorithm, aBuffer, aOffset, aLength, mBlockDevice.getGeneration());

			int blockSize = mBlockDevice.getBlockSize();
			byte[] output = null;
			int physicalSize = aLength;

			if (aCompressorLevel != CompressorAlgorithm.NONE)
			{
				ByteBlockOutputStream tmp = new ByteBlockOutputStream(blockSize);
				if (CompressorAlgorithm.compress(aCompressorLevel, aBuffer, aOffset, aLength, tmp))
				{
					output = tmp.getBuffer();
					physicalSize = tmp.size();
					if (output.length < roundUp(output.length))
					{
						output = Arrays.copyOfRange(output, 0, roundUp(output.length));
					}
					if (output.length >= roundUp(aBuffer.length) - blockSize)
					{
						output = null;
					}
				}
			}

			if (output == null)
			{
				aCompressorLevel = CompressorAlgorithm.NONE;
				output = new byte[roundUp(aLength)];
				System.arraycopy(aBuffer, aOffset, output, 0, aLength);
				physicalSize = aLength;
			}

			assert output.length % blockSize == 0 : output.length;

			long blockIndex = mBlockDevice.allocBlock(output.length / blockSize);

			blockPointer = new BlockPointer()
				.setBlockType(aBlockType)
				.setBlockLevel(aBlockLevel)
				.setCompressionAlgorithm(aCompressorLevel)
				.setChecksumAlgorithm(mChecksumAlgorithm)
				.setAllocatedSize(output.length)
				.setPhysicalSize(physicalSize)
				.setLogicalSize(aLength)
				.setBlockIndex0(blockIndex)
				.setBlockKey(createBlockKey())
				.setChecksum(checksum)
				.setGeneration(mBlockDevice.getGeneration());

			Log.d("write block %S", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, output, 0, output.length, blockPointer.getBlockKey());

//			assert collectStatistics(WRITE_BLOCK, aBuffer.length);
			Log.dec();

			return blockPointer;
		}
		catch (Exception | Error e)
		{
			throw new RaccoonIOException("Error writing block: " + blockPointer, e);
		}
	}


	private static int[] createBlockKey()
	{
		return new int[]{PRNG.nextInt(), PRNG.nextInt(), PRNG.nextInt(), PRNG.nextInt()};
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


	private boolean isAllZeros(byte[] aBuffer, int aOffset, int aLength)
	{
		for (int i = 0; i < aLength; i++)
		{
			if (aBuffer[aOffset + i] != 0)
			{
				return false;
			}
		}
		return true;
	}
}
