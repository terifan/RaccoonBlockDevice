package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.util.Log;


public class LobByteChannel implements SeekableByteChannel
{
	private final static boolean LOG = false;

	private final static int MAX_BLOCK_SIZE = 1024 * 1024;
	private final static int HEADER_SIZE = 8;
	private final static int INDIRECT_POINTER_THRESHOLD = 4;
	private final static int BLOCKTYPE_DATA = 0;
	private final static int BLOCKTYPE_INDEX = 1;

	private BlockAccessor mBlockAccessor;
	private ByteArrayBuffer mPersistedPointerBuffer;
	private ByteArrayBuffer mPersistedIndirectPointerBuffer;
	private HashMap<Integer,BlockPointer> mPendingBlockPointsers;
	private HashSet<Integer> mEmptyBlocks;
	private boolean mClosed;
	private Listener<LobByteChannel> mCloseListener;

	private long mTotalSize;
	private long mPosition;
	private byte[] mBuffer;
	private boolean mModified;
	private boolean mChunkModified;
	private int mChunkIndex;
	private BlockPointer mBlockPointer;
	private byte[] mHeader;
	private CompressorLevel mInteriorBlockCompressor;
	private CompressorLevel mLeafBlockCompressor;


	public LobByteChannel(BlockAccessor aBlockAccessor, byte[] aHeader, LobOpenOption aOpenOption, Listener<LobByteChannel> aListener) throws IOException
	{
		mHeader = aHeader;
		mCloseListener = aListener;
		mBlockAccessor = aBlockAccessor;

		mInteriorBlockCompressor = CompressorLevel.ZLE;
		mLeafBlockCompressor = CompressorLevel.NONE;

		if (aOpenOption == LobOpenOption.REPLACE && aHeader != null)
		{
			delete();
		}

		mBuffer = new byte[MAX_BLOCK_SIZE];
		mPendingBlockPointsers = new HashMap<>();
		mEmptyBlocks = new HashSet<>();

		if (mHeader != null)
		{
			mPersistedPointerBuffer = ByteArrayBuffer.wrap(mHeader);
			mTotalSize = mPersistedPointerBuffer.readInt64();

			BlockPointer bp = new BlockPointer();
			bp.unmarshal(mPersistedPointerBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				mPersistedIndirectPointerBuffer = mPersistedPointerBuffer;
				mPersistedPointerBuffer = ByteArrayBuffer.wrap(mBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize());
			}
		}

		if (aOpenOption == LobOpenOption.APPEND)
		{
			mPosition = mTotalSize;
		}
		if (aOpenOption == LobOpenOption.APPEND || aOpenOption == LobOpenOption.WRITE)
		{
			mChunkIndex = -1; // force sync to load the block at mPosition
		}
	}


	@Override
	public int read(ByteBuffer aDst) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		int limit = aDst.limit() - aDst.position();

		int total = (int)Math.min(limit, mTotalSize - mPosition);

		if (total == 0)
		{
			return -1;
		}

		if (LOG) System.out.println("READ  " + mPosition + " +" + total);

		int posInChunk = (int)(mPosition % MAX_BLOCK_SIZE);

		for (int remaining = total; remaining > 0; )
		{
			sync(false);

			int length = Math.min(remaining, MAX_BLOCK_SIZE - posInChunk);

			if (LOG) System.out.println("\tAppend " + posInChunk + " +" + length);

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

		if (LOG) System.out.println("WRITE " + mPosition + " +" + total);

		int posInChunk = (int)(mPosition % MAX_BLOCK_SIZE);

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, MAX_BLOCK_SIZE - posInChunk);

			if (LOG) System.out.println("\tAppend " + posInChunk + " +" + length);

			aSrc.get(mBuffer, posInChunk, length);
			mPosition += length;
			remaining -= length;

			mTotalSize = Math.max(mTotalSize, mPosition);
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
		return mTotalSize;
	}


	@Override
	public SeekableByteChannel truncate(long aSize) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		throw new UnsupportedOperationException();

//		if (aSize < mSize)
//		{
//			sync(true);
//
//			for (long partIndex = aSize / Blob.CHUNK_SIZE; partIndex < mSize / Blob.CHUNK_SIZE; partIndex++)
//			{
//				mFile.mFileSystem.getDatabase().remove(new BlobPtr(mFile.getObjectId(), partIndex));
//			}
//
//			if ((aSize % CHUNK_SIZE) > 0)
//			{
//				long ci = aSize / CHUNK_SIZE;
//
//				byte[] tmp = Streams.readAll(mFile.mFileSystem.getDatabase().load(new BlobPtr(mFile.getObjectId(), ci)));
//
//				Arrays.fill(tmp, CHUNK_SIZE - (int)(aSize % CHUNK_SIZE), CHUNK_SIZE, (byte)0);
//
//				mFile.mFileSystem.getDatabase().save(new BlobPtr(mFile.getObjectId(), ci), new ByteArrayInputStream(mBuffer, 0, (int)(mSize % CHUNK_SIZE)));
//			}
//
//			mSize = aSize;
//
//			if (mPosition > mSize)
//			{
//				mPosition = mSize;
//			}
//		}
//		return this;
	}


	@Override
	public boolean isOpen()
	{
		return !mClosed;
	}


	public synchronized byte[] finish() throws IOException
	{
		if (mClosed || !mModified)
		{
			return mHeader;
		}

		sync(true);

		Log.d("closing blob");
		Log.inc();

		int pointerCount = (int)((mTotalSize + MAX_BLOCK_SIZE - 1) / MAX_BLOCK_SIZE);
		ByteArrayBuffer buf = ByteArrayBuffer.alloc(HEADER_SIZE + BlockPointer.SIZE * pointerCount, true);
		buf.limit(buf.capacity());

		if (LOG) System.out.println("CLOSE +" + mTotalSize);

		buf.writeInt64(mTotalSize);

		for (int i = 0; i < pointerCount; i++)
		{
			if (mEmptyBlocks.contains(i))
			{
				if (LOG) System.out.println("\tempty");
				buf.write(new byte[BlockPointer.SIZE]);
			}
			else
			{
				BlockPointer bp = mPendingBlockPointsers.get(i);
				if (bp != null)
				{
					bp.marshal(buf);
					if (LOG) System.out.println("\tnew " + bp);
				}
				else if (mPersistedPointerBuffer != null && mPersistedPointerBuffer.capacity() > buf.position())
				{
					mPersistedPointerBuffer.position(HEADER_SIZE + i * BlockPointer.SIZE).copyTo(buf, BlockPointer.SIZE);
					if (LOG) System.out.println("\told " + new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mPersistedPointerBuffer.array()).position(HEADER_SIZE + i * BlockPointer.SIZE)));
				}
			}
		}

		if (pointerCount > INDIRECT_POINTER_THRESHOLD)
		{
			Log.d("created indirect blob pointer block");
			Log.inc();

			buf.position(HEADER_SIZE);
			BlockPointer bp = mBlockAccessor.writeBlock(buf.array(), 0, buf.capacity(), BLOCKTYPE_INDEX, 0, mInteriorBlockCompressor);
			bp.marshal(buf);
			buf.trim();

			Log.dec();
		}

		if (mPersistedIndirectPointerBuffer != null)
		{
			Log.d("freed indirect block");
			Log.inc();

			BlockPointer bp = new BlockPointer();
			bp.unmarshal(mPersistedIndirectPointerBuffer.position(HEADER_SIZE));
			mBlockAccessor.freeBlock(bp);

			Log.dec();
		}

		mHeader = buf.array();
		mClosed = true;

		Log.dec();

		return mHeader;
	}


	@Override
	public void close() throws IOException
	{
		if (mCloseListener != null)
		{
			mCloseListener.call(this);
		}
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG) System.out.println("\tsync pos: " + mPosition + ", size: " + mTotalSize + ", final: " + aFinal + ", posChunk: " + (mPosition % MAX_BLOCK_SIZE) + ", indexChunk:" + mPosition / MAX_BLOCK_SIZE + ", mod: " + mChunkModified);

		if ((mPosition % MAX_BLOCK_SIZE) == 0 || mPosition / MAX_BLOCK_SIZE != mChunkIndex || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(MAX_BLOCK_SIZE, mTotalSize - MAX_BLOCK_SIZE * mChunkIndex);

				if (LOG) System.out.println("\tWriting chunk " + mChunkIndex + " " + len + " bytes");

				BlockPointer old = mPendingBlockPointsers.remove(mChunkIndex);
				if (old != null)
				{
					mBlockAccessor.freeBlock(old);
					mEmptyBlocks.remove(mChunkIndex);
				}

				if (isAllZeros())
				{
					mEmptyBlocks.add(mChunkIndex);
				}
				else
				{
					BlockPointer bp = mBlockAccessor.writeBlock(mBuffer, 0, len, BLOCKTYPE_DATA, 0, mLeafBlockCompressor);
					mPendingBlockPointsers.put(mChunkIndex, bp);
				}
			}

			if (!aFinal)
			{
				Arrays.fill(mBuffer, (byte)0);

				mChunkIndex = (int)(mPosition / MAX_BLOCK_SIZE);
				mChunkModified = false;

				mBlockPointer = mPendingBlockPointsers.get(mChunkIndex);

				if (mBlockPointer == null && !mEmptyBlocks.contains(mChunkIndex))
				{
					int o = HEADER_SIZE + mChunkIndex * BlockPointer.SIZE;

					if (mPersistedPointerBuffer != null && mPersistedPointerBuffer.capacity() > o)
					{
						mBlockPointer = new BlockPointer();
						mBlockPointer.unmarshal(mPersistedPointerBuffer.position(o));
					}
				}

				if (mBlockPointer != null)
				{
					if (LOG) System.out.println("\tReading chunk " + mChunkIndex);

					byte[] tmp = mBlockAccessor.readBlock(mBlockPointer);
					System.arraycopy(tmp, 0, mBuffer, 0, tmp.length);
				}
			}
		}
	}


	private boolean isAllZeros()
	{
		for (int i = 0; i < mBuffer.length; i++)
		{
			if (mBuffer[i] != 0)
			{
				return false;
			}
		}
		return true;
	}


	public void delete()
	{
		freeBlocks(ByteArrayBuffer.wrap(mHeader).position(HEADER_SIZE));
	}


	private void freeBlocks(ByteArrayBuffer aBuffer)
	{
		while (aBuffer.remaining() > 0)
		{
			BlockPointer bp = new BlockPointer().unmarshal(aBuffer);

			if (bp.getBlockType() == BlockType.BLOB_INDEX)
			{
				freeBlocks(ByteArrayBuffer.wrap(mBlockAccessor.readBlock(bp)).limit(bp.getLogicalSize()).position(HEADER_SIZE));
			}

			mBlockAccessor.freeBlock(bp);
		}
	}


	public byte[] readAllBytes() throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate((int)size());
		read(buf);
		return buf.array();
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
