package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.compressor.ByteBlockOutputStream;
import org.terifan.raccoon.blockdevice.compressor.Compressor;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.security.messagedigest.MurmurHash3;
import org.terifan.raccoon.security.random.ISAAC;


public class BlockAccessor implements AutoCloseable
{
	private final static ISAAC PRNG = new ISAAC();
	private final ManagedBlockDevice mBlockDevice;
	private final boolean mCloseUnderlyingDevice;


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

			mBlockDevice.readBlock(aBlockPointer.getBlockIndex0(), buffer, 0, buffer.length, aBlockPointer.getBlockKey());

			int[] hash;
			if (aBlockPointer.getChecksumAlgorithm() == 0)
			{
				hash = MurmurHash3.hash128(buffer, 0, aBlockPointer.getPhysicalSize(), aBlockPointer.getGeneration());
			}
			else
			{
				throw new IOException("Unsupported checksum algorithm");
			}

			if (!aBlockPointer.verifyChecksum(hash))
			{
				throw new IOException("Checksum error in block " + aBlockPointer);
			}

			if (aBlockPointer.getCompressionAlgorithm() != CompressorLevel.NONE.ordinal())
			{
				byte[] tmp = new byte[aBlockPointer.getLogicalSize()];
				Compressor compressor = CompressorLevel.values()[aBlockPointer.getCompressionAlgorithm()].newInstance();
				compressor.decompress(buffer, 0, aBlockPointer.getPhysicalSize(), tmp, 0, aBlockPointer.getLogicalSize());
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


	public synchronized BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, int aBlockType, int aBlockLevel, CompressorLevel aCompressorLevel)
	{
		BlockPointer blockPointer = null;

		try
		{
			int blockSize = mBlockDevice.getBlockSize();
			byte[] compressedBlock = null;

			if (aCompressorLevel != CompressorLevel.NONE)
			{
				ByteBlockOutputStream tmp = new ByteBlockOutputStream(blockSize);
				if (aCompressorLevel.newInstance().compress(aBuffer, aOffset, aLength, tmp))
				{
					compressedBlock = tmp.getBuffer();
					assert (compressedBlock.length % blockSize) == 0;
				}
			}

			int physicalSize;
			if (compressedBlock != null && compressedBlock.length <= roundUp(aBuffer.length) - blockSize) // use the compressed result only if we actual save one block or more
			{
				physicalSize = compressedBlock.length;
				aBuffer = compressedBlock;
			}
			else
			{
				physicalSize = aLength;
				aBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + roundUp(aLength));
				aCompressorLevel = CompressorLevel.NONE;
			}

			assert aBuffer.length % blockSize == 0;

			long blockIndex = mBlockDevice.allocBlock(aBuffer.length / blockSize);

			blockPointer = new BlockPointer()
				.setBlockType(aBlockType)
				.setBlockLevel(aBlockLevel)
				.setCompressionAlgorithm(aCompressorLevel.ordinal())
				.setChecksumAlgorithm((byte)0)
				.setAllocatedSize(aBuffer.length)
				.setPhysicalSize(physicalSize)
				.setLogicalSize(aLength)
				.setBlockIndex0(blockIndex)
				.setBlockKey(createBlockKey())
				.setChecksum(MurmurHash3.hash128(aBuffer, 0, physicalSize, mBlockDevice.getGeneration()))
				.setGeneration(mBlockDevice.getGeneration());

			Log.d("write block %s", blockPointer);
			Log.inc();

			mBlockDevice.writeBlock(blockIndex, aBuffer, 0, aBuffer.length, blockPointer.getBlockKey());

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
}
