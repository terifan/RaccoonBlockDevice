package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


public class LobByteChannel implements SeekableByteChannel
{
	private final static boolean LOG = false;

	private final static int INDIRECT_POINTER_THRESHOLD = 4;

	private LobHeader mLobHeader;
	private BlockAccessor mBlockAccessor;
	private ArrayList<BlockPointer> mBlockPointers;
	private boolean mClosed;
	private long mTotalSize;
	private long mPosition;
	private byte[] mBuffer;
	private boolean mModified;
	private boolean mChunkModified;
	private int mChunkIndex;
	private int mLeafBlockSize;
	private CompressorLevel mInteriorBlockCompressor;
	private CompressorLevel mLeafBlockCompressor;
	private BlockPointer mIndirectBlockPointer;
	private Runnable mCloseAction;


	public LobByteChannel(BlockAccessor aBlockAccessor, LobHeader aLobHeader, LobOpenOption aOpenOption) throws IOException
	{
		this(aBlockAccessor, aLobHeader, aOpenOption, ()->{});
	}


	public LobByteChannel(BlockAccessor aBlockAccessor, LobHeader aLobHeader, LobOpenOption aOpenOption, Runnable aCloseAction) throws IOException
	{
		mLobHeader = aLobHeader;
		mBlockAccessor = aBlockAccessor;
		mCloseAction = aCloseAction;

		mInteriorBlockCompressor = CompressorLevel.ZLE;
		mLeafBlockCompressor = CompressorLevel.NONE;

		if (aOpenOption == LobOpenOption.REPLACE && aLobHeader != null)
		{
			delete();
		}

		if (mLobHeader.mData == null)
		{
			mLobHeader.mData = new Document();
		}

		mLeafBlockSize = mLobHeader.mData.get("blockSize", 1 << 20);

		if ((mLeafBlockSize & (mLeafBlockSize - 1)) != 0)
		{
			throw new IllegalArgumentException("BlockSize must be power of 2: " + mLeafBlockSize);
		}

		mBuffer = new byte[mLeafBlockSize];
		mTotalSize = mLobHeader.mData.get("totalSize", 0L);
		mBlockPointers = new ArrayList<>();

		Array pointers = mLobHeader.mData.getArray("pointers");
		if (pointers != null && pointers.size() > 0)
		{
			BlockPointer tmp = new BlockPointer().unmarshalDoc(pointers.getDocument(0));

			if (tmp.getBlockType() == BlockType.LOB_INDEX)
			{
				mIndirectBlockPointer = tmp;
				for (Document doc : new Array().fromByteArray(mBlockAccessor.readBlock(mIndirectBlockPointer)).iterator(Document.class))
				{
					mBlockPointers.add(new BlockPointer().unmarshalDoc(doc));
				}
			}
			else
			{
				for (Document data : pointers.iterator(Document.class))
				{
					mBlockPointers.add(new BlockPointer().unmarshalDoc(data));
				}
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
			System.out.println("CLOSE +" + mTotalSize);
		}

		for (int i = 0; i < mBlockPointers.size(); i++)
		{
			BlockPointer bp = mBlockPointers.get(i);
			pointers.add(bp.marshalDoc());
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

		if (mBlockPointers.size() > INDIRECT_POINTER_THRESHOLD)
		{
			Log.d("created indirect lob pointer block");
			Log.inc();

			byte[] buf = pointers.toByteArray();
			mIndirectBlockPointer = mBlockAccessor.writeBlock(buf, 0, buf.length, BlockType.LOB_INDEX, 1, mInteriorBlockCompressor);

			pointers.clear();
			pointers.add(mIndirectBlockPointer.marshalDoc());

			Log.dec();
		}

		mLobHeader.mData = new Document().put("totalSize", mTotalSize).put("blockSize", mLeafBlockSize).put("pointers", pointers);
		mClosed = true;

		if (mCloseAction != null)
		{
			mCloseAction.run();
			mCloseAction = null;
		}

		Log.dec();
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG)
		{
			System.out.println("\tsync pos: " + mPosition + ", size: " + mTotalSize + ", final: " + aFinal + ", posChunk: " + posInChunk() + ", indexChunk:" + mPosition / mLeafBlockSize + ", mod: " + mChunkModified);
		}

		if (posInChunk() == 0 || mChunkIndex != mPosition / mLeafBlockSize || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(mLeafBlockSize, mTotalSize - mLeafBlockSize * mChunkIndex);

				if (LOG)
				{
					System.out.println("\tWriting chunk " + mChunkIndex + " " + len + " bytes");
				}

				BlockPointer bp = mChunkIndex >= mBlockPointers.size() ? null : mBlockPointers.get(mChunkIndex);

				if (bp != null)
				{
					mBlockAccessor.freeBlock(bp);
				}

				if (isAllZeros())
				{
					bp = new BlockPointer().setLogicalSize(len).setBlockType(BlockType.HOLE);
				}
				else
				{
					bp = mBlockAccessor.writeBlock(mBuffer, 0, len, BlockType.LOB_LEAF, 0, mLeafBlockCompressor);
				}

				if (mChunkIndex == mBlockPointers.size())
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

				if (bp != null && bp.getBlockType() != BlockType.HOLE)
				{
					if (LOG)
					{
						System.out.println("\tReading chunk " + mChunkIndex);
					}

					byte[] tmp = mBlockAccessor.readBlock(bp);
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
		for (BlockPointer bp : mBlockPointers)
		{
			if (bp.getBlockType() != BlockType.HOLE)
			{
				mBlockAccessor.freeBlock(bp);
			}
		}

		if (mIndirectBlockPointer != null)
		{
			mBlockAccessor.freeBlock(mIndirectBlockPointer);
		}

		mTotalSize = 0;
		mBlockPointers.clear();
		mIndirectBlockPointer = null;
		mPosition = 0;
		Arrays.fill(mBuffer, (byte)0);
		mModified = true;
		mChunkModified = false;
		mChunkIndex = 0;
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


	private int posInChunk()
	{
		return (int)(mPosition & (mLeafBlockSize - 1));
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
