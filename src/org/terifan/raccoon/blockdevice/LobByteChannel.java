package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


// lob pointer		version, block size, length, compression, [ptr]
// lob directory	[ptr][ptr][ptr][...][...][...][...][...][ptr][ptr][...][...][...][ptr][...][ptr]
// lob blocks		##### ##### ##### ##### ##### ##### #####

// 1024*1024*1024 / 1048576 =   1024 * 80 =     81,920
// 1024*1024*1024 /  262144 =   4096 * 80 =    327,680
// 1024*1024*1024 /   65536 =  16384 * 80 =  1,310,720
// 1024*1024*1024 /   16384 =  65536 * 80 =  5,242,880
// 1024*1024*1024 /    4096 = 262144 * 80 = 20,971,520

public class LobByteChannel implements SeekableByteChannel
{
	private final static boolean LOG = !false;

	public final static int DEFAULT_LEAF_SIZE = 0x100000;

	private final static String LENGTH = "0";
	private final static String BLOCK_SIZE = "1";
	private final static String POINTERS = "2";
	private final static String METADATA = "3";

	private final static int INDIRECT_PTR_THRESHOLD = 3;

	public final static String METADATA_MODIFIED = "$modified";
	public final static String METADATA_CREATED = "$created";
	public final static String METADATA_LENGTH = "$length";

	private Document mHeader;
	private BlockAccessor mBlockAccessor;
	private ArrayList<BlockPointer> mBlockPointers;
	private boolean mClosed;
	private long mLength;
	private long mPosition;
	private byte[] mBuffer;
	private boolean mModified;
	private boolean mChunkModified;
	private int mChunkIndex;
	private int mLeafBlockSize;
	private CompressorLevel mCompressor;
	private BlockPointer mIndirectBlockPointer;
	private Consumer<LobByteChannel> mCloseAction;
	private int mIndirectPointerThreshold;
	private boolean mWriteMetadata;


	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption) throws IOException
	{
		this(aBlockAccessor, aHeader, aOpenOption, INDIRECT_PTR_THRESHOLD, true, DEFAULT_LEAF_SIZE);
	}


	@Deprecated
	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption, int aIndirectPointerThreshold, boolean aWriteMetadata, int aLeafBlockSize) throws IOException
	{
		if (aHeader == null)
		{
			throw new IllegalArgumentException();
		}

		mHeader = aHeader;
		mBlockAccessor = aBlockAccessor;
		mCloseAction = e->{};
		mCompressor = CompressorLevel.NONE;
		mIndirectPointerThreshold = aIndirectPointerThreshold;
		mWriteMetadata = aWriteMetadata;

		if (aOpenOption == LobOpenOption.REPLACE)
		{
			delete();
		}

		mLeafBlockSize = mHeader.get(BLOCK_SIZE, () -> {int bs = mBlockAccessor.getBlockDevice().getBlockSize(); return aLeafBlockSize / bs * bs;});

		if ((mLeafBlockSize & (mLeafBlockSize - 1)) != 0)
		{
			throw new IllegalArgumentException("BlockSize must be power of 2: " + mLeafBlockSize);
		}

		if (mWriteMetadata)
		{
			LocalDateTime now = LocalDateTime.now();
			getMetadata()
				.putIfAbsent(METADATA_CREATED, () -> now)
				.putIfAbsent(METADATA_MODIFIED, () -> now)
				.putIfAbsent(METADATA_LENGTH, () -> mLength);
		}

		mBuffer = new byte[mLeafBlockSize];
		mLength = mHeader.get(LENGTH, 0L);
		mBlockPointers = new ArrayList<>();

		Array pointers = mHeader.getArray(POINTERS);
		if (pointers != null && !pointers.isEmpty())
		{
			BlockPointer tmp = new BlockPointer().unmarshal(pointers.get(0));

			if (tmp.getBlockType() == BlockType.LOB_NODE)
			{
				mIndirectBlockPointer = tmp;
				for (byte[] data : new Array().fromByteArray(mBlockAccessor.readBlock(mIndirectBlockPointer)).iterable(byte[].class))
				{
					mBlockPointers.add(new BlockPointer().unmarshal(data));
				}
			}
			else
			{
				for (byte[] data : pointers.iterable(byte[].class))
				{
					mBlockPointers.add(new BlockPointer().unmarshal(data));
				}
			}
		}

		if (aOpenOption == LobOpenOption.APPEND)
		{
			mPosition = mLength;
		}
		if (aOpenOption == LobOpenOption.APPEND || aOpenOption == LobOpenOption.WRITE)
		{
			mChunkIndex = -1; // force sync to load the block at mPosition
		}
	}


	public LobByteChannel setCloseAction(Consumer<LobByteChannel> aCloseAction)
	{
		mCloseAction = aCloseAction == null ? e->{} : aCloseAction;
		return this;
	}


	@Override
	public int read(ByteBuffer aDst) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		int limit = aDst.limit() - aDst.position();

		int total = (int)Math.min(limit, mLength - mPosition);

		if (total == 0)
		{
			return -1;
		}

		if (LOG)
		{
			System.out.println("READ  " + mPosition + " +" + total);
		}

		int posInChunk = posInChunk();

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, mLeafBlockSize - posInChunk);

			if (LOG)
			{
				System.out.println("\tAppend " + posInChunk + " +" + length);
			}

			aDst.put(mBuffer, posInChunk, length);
			mPosition += length;
			remaining -= length;

			posInChunk = 0;
		}

		return total;
	}


	@Override
	public int write(ByteBuffer aSrc) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		int total = aSrc.limit() - aSrc.position();

		if (LOG)
		{
			System.out.println("WRITE " + mPosition + " +" + total);
		}

		int posInChunk = posInChunk();

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, mLeafBlockSize - posInChunk);

			if (LOG)
			{
				System.out.println("\tAppend " + posInChunk + " +" + length);
			}

			aSrc.get(mBuffer, posInChunk, length);
			mPosition += length;
			remaining -= length;

			mLength = Math.max(mLength, mPosition);
			mChunkModified = true;
			posInChunk = 0;
		}

		mModified = true;

		return total;
	}


	@Override
	public long position() throws IOException
	{
		return mPosition;
	}


	@Override
	public SeekableByteChannel position(long aNewPosition) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		mPosition = aNewPosition;
		return this;
	}


	@Override
	public long size() throws IOException
	{
		return mLength;
	}


	@Override
	public SeekableByteChannel truncate(long aSize) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		throw new UnsupportedOperationException();
	}


	@Override
	public boolean isOpen()
	{
		return !mClosed;
	}


	@Override
	public void close() throws IOException
	{
		if (mClosed || !mModified)
		{
			return;
		}

		sync(true);

		Log.d("closing lob");
		Log.inc();

		Array pointers = new Array();

		if (LOG)
		{
			System.out.println("CLOSE +" + mLength);
		}

		for (int i = 0; i < mBlockPointers.size(); i++)
		{
			BlockPointer bp = mBlockPointers.get(i);
			pointers.add(bp.marshal());
			if (LOG)
			{
				System.out.println("\tnew " + bp);
			}
		}

		if (mIndirectBlockPointer != null)
		{
			Log.d("freed indirect block");
			Log.inc();

			mBlockAccessor.freeBlock(mIndirectBlockPointer);
			mIndirectBlockPointer = null;

			Log.dec();
		}

		if (mBlockPointers.size() > mIndirectPointerThreshold)
		{
			Log.d("created indirect lob pointer block");
			Log.inc();

			byte[] buf = pointers.toByteArray();
			mIndirectBlockPointer = mBlockAccessor.writeBlock(buf, 0, buf.length, BlockType.LOB_NODE, 1, CompressorLevel.ZLE);

			pointers.clear();
			pointers.add(mIndirectBlockPointer.marshal());

			Log.dec();
		}

		mHeader.put(LENGTH, mLength).put(BLOCK_SIZE, mLeafBlockSize).put(POINTERS, pointers);

		if (mWriteMetadata)
		{
			getMetadata()
				.putIfAbsent(METADATA_CREATED, LocalDateTime::now)
				.put(METADATA_MODIFIED, LocalDateTime.now())
				.put(METADATA_LENGTH, mLength);
		}

		mClosed = true;

		if (mCloseAction != null)
		{
			mCloseAction.accept(this);
		}

		mBlockAccessor = null;
		mBlockPointers = null;
		mCloseAction = null;
		mBuffer = null;
		mHeader = null;
		mIndirectBlockPointer = null;

		Log.dec();
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG)
		{
			System.out.println("\tSync pos: " + mPosition + ", size: " + mLength + ", final: " + aFinal + ", posChunk: " + posInChunk() + ", indexChunk:" + mPosition / mLeafBlockSize + ", mod: " + mChunkModified);
		}

		if (posInChunk() == 0 || mChunkIndex != mPosition / mLeafBlockSize || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(mLeafBlockSize, mLength - mLeafBlockSize * mChunkIndex);

				if (LOG)
				{
					System.out.println("\tWrite chunk " + mChunkIndex + ", " + len + " bytes");
				}

				BlockPointer bp = mChunkIndex >= mBlockPointers.size() ? null : mBlockPointers.get(mChunkIndex);

				mBlockAccessor.freeBlock(bp);

				bp = mBlockAccessor.writeBlock(mBuffer, 0, len, BlockType.LOB_LEAF, 0, mCompressor);

				if (mChunkIndex >= mBlockPointers.size())
				{
					mBlockPointers.add(bp);
				}
				else
				{
					mBlockPointers.set(mChunkIndex, bp);
				}
			}

			if (!aFinal)
			{
				Arrays.fill(mBuffer, (byte)0);

				mChunkIndex = (int)(mPosition / mLeafBlockSize);
				mChunkModified = false;

				BlockPointer bp = mChunkIndex >= mBlockPointers.size() ? null : mBlockPointers.get(mChunkIndex);

				if (bp != null)
				{
					if (LOG)
					{
						System.out.println("\tRead chunk " + mChunkIndex);
					}

					byte[] tmp = mBlockAccessor.readBlock(bp);
					System.arraycopy(tmp, 0, mBuffer, 0, tmp.length);
				}
			}
		}
	}


	public void delete()
	{
		if (mBlockPointers != null)
		{
			for (BlockPointer bp : mBlockPointers)
			{
				mBlockAccessor.freeBlock(bp);
			}
			mBlockPointers.clear();
		}

		mBlockAccessor.freeBlock(mIndirectBlockPointer);

		mLength = 0;
		mIndirectBlockPointer = null;
		mPosition = 0;
		mModified = true;
		mChunkModified = false;
		mChunkIndex = 0;
		mClosed = true;

		if (mBuffer != null)
		{
			Arrays.fill(mBuffer, (byte)0);
		}
	}


	public byte[] readAllBytes() throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate((int)size());
		read(buf);
		return buf.array();
	}


	public byte[] readAllBytes(byte[] aBuffer) throws IOException
	{
		ByteBuffer buf = ByteBuffer.wrap(aBuffer);
		read(buf);
		return aBuffer;
	}


	public LobByteChannel readAllBytes(OutputStream aDst) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(1024);

		for (long remaining = size(); remaining > 0;)
		{
			int len = read(buf);
			if (len == -1)
			{
				throw new IOException("Unexpected end of stream");
			}
			aDst.write(buf.array(), 0, len);
		}

		return this;
	}


	public LobByteChannel writeAllBytes(byte[] aSrc) throws IOException
	{
		write(ByteBuffer.wrap(aSrc));
		return this;
	}


	public LobByteChannel writeAllBytes(InputStream aSrc) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(1024);

		for (int len; (len = aSrc.read(buf.array())) > 0;)
		{
			buf.position(0).limit(len);
			write(buf);
		}

		return this;
	}


	public InputStream newInputStream()
	{
		return new InputStream()
		{
			long streamPosition = mPosition;
			ByteBuffer buffer = (ByteBuffer)ByteBuffer.allocate(4096).position(4096);


			@Override
			public int read() throws IOException
			{
				if (buffer.position() == buffer.limit())
				{
					if (streamPosition == size())
					{
						return -1;
					}

					position(streamPosition);
					buffer.position(0);
					LobByteChannel.this.read(buffer);
					buffer.flip();
					streamPosition = position();
				}

				return 0xff & buffer.get();
			}


			@Override
			public int read(byte[] aBuffer, int aOffset, int aLength) throws IOException
			{
				int total = 0;

				for (int remaining = aLength; remaining > 0;)
				{
					if (buffer.position() == buffer.limit())
					{
						if (streamPosition == size())
						{
							break;
						}

						position(streamPosition);
						buffer.position(0);
						LobByteChannel.this.read(buffer);
						buffer.flip();
						streamPosition = position();
					}

					int len = Math.min(remaining, buffer.remaining());

					buffer.get(aBuffer, aOffset + total, len);
					remaining -= len;
					total += len;
				}

				return total == 0 ? -1 : total;
			}


			@Override
			public void close() throws IOException
			{
				LobByteChannel.this.close();
			}
		};
	}


	public OutputStream newOutputStream()
	{
		return new OutputStream()
		{
			ByteBuffer buffer = (ByteBuffer)ByteBuffer.allocate(4096);


			@Override
			public void write(int aByte) throws IOException
			{
				buffer.put((byte)aByte);

				if (buffer.position() == buffer.capacity())
				{
					buffer.flip();
					LobByteChannel.this.write(buffer);
					buffer.clear();
				}
			}


			@Override
			public void write(byte[] aBuffer, int aOffset, int aLength) throws IOException
			{
				for (int remaining = aLength; remaining > 0;)
				{
					int len = Math.min(remaining, buffer.remaining());

					buffer.put(aBuffer, aOffset, len);

					remaining -= len;
					aOffset += len;

					if (buffer.position() == buffer.capacity())
					{
						buffer.flip();
						LobByteChannel.this.write(buffer);
						buffer.clear();
					}
				}
			}


			@Override
			public void close() throws IOException
			{
				if (buffer.position() > 0)
				{
					buffer.flip();
					LobByteChannel.this.write(buffer);
				}

				LobByteChannel.this.close();
			}
		};
	}


	public boolean isModified()
	{
		return mModified;
	}


	private int posInChunk()
	{
		return (int)(mPosition & (mLeafBlockSize - 1));
	}


	/**
	 * @return the metadata Document with information that is stored along with the LOB. Fields with "$" prefix will be updated by the
	 * implementation.
	 */
	public Document getMetadata()
	{
		return mHeader.computeIfAbsent(METADATA, () -> new Document());
	}

//	void scan(ScanResult aScanResult)
//	{
//		aScanResult.enterBlob();
//
//		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(mHeader);
//		aScanResult.blobs++;
//		aScanResult.blobLogicalSize += buffer.readInt64();
//
//		BlockPointer bp = new BlockPointer();
//		bp.unmarshal(buffer);
//
//		if (bp.getBlockType() == BlockType.BLOB_INDEX)
//		{
//			aScanResult.blobIndirect(bp);
//			aScanResult.blobIndirectBlocks++;
//
//			buffer = ByteArrayBuffer.wrap(mBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize());
//		}
//
//		buffer.position(HEADER_SIZE);
//
//		while (buffer.remaining() > 0)
//		{
//			bp.unmarshal(buffer);
//
//			aScanResult.blobData(bp);
//
//			aScanResult.blobDataBlocks++;
//			aScanResult.blobAllocatedSize += bp.getAllocatedSize();
//			aScanResult.blobPhysicalSize += bp.getAllocatedSize(); // why not phys size???
//		}
//
//		aScanResult.exitBlob();
//	}
}
