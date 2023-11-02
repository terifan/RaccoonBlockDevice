package org.terifan.raccoon.blockdevice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.function.Consumer;
import static org.terifan.raccoon.blockdevice.BlockPointer.SIZE;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;

//                       __________node____________
//                      /                          \
//         ___________node________                hole
//        /      |                \
//      hole  __node__          __node__
//           /   |    \        /   |    \
//         leaf hole leaf    leaf leaf leaf

//
// lob pointer		version, logical size, block size, compression, [ptr]
// lob directory	[ptr][ptr][ptr][...][...][...][...][...][ptr][ptr][...][...][...][ptr][...][ptr]
// lob blocks		##### ##### ##### ##### ##### ##### #####
//
// 1024 * 1024 * 1024 / 131,072 = 8,192 * 80 = 655,360
//
public class LobByteChannel implements SeekableByteChannel
{
	private final static boolean LOG = !false;

	public final static int DEFAULT_LEAF_SIZE = 128 * 1024;
	public final static int DEFAULT_COMPRESSOR = CompressorAlgorithm.NONE;

	private final static String IX_VERSION = "0";
	private final static String IX_LOGICAL_SIZE = "1";
	private final static String IX_BLOCK_SIZE = "2";
	private final static String IX_COMPRESSION = "3";
	private final static String IX_POINTER = "4";
	private final static String IX_METADATA = "5";

	public final static String METADATA_MODIFIED = "$modified";
	public final static String METADATA_CREATED = "$created";
	public final static String METADATA_PHYSICAL_SIZE = "$physical";
	public final static String METADATA_ALLOCATED_SIZE = "$allocated";
	public final static String METADATA_LOGICAL_SIZE = "$logical";

	private Document mHeader;
	private BlockAccessor mBlockAccessor;
	private ByteArrayBuffer mIndirectBlock;
	private boolean mClosed;
	private long mLength;
	private long mPosition;
	private boolean mModified;
	private byte[] mChunk;
	private int mChunkIndex;
	private boolean mChunkModified;
	private int mLeafBlockSize;
	private int mCompressor;
	private BlockPointer mIndirectBlockPointer;
	private Consumer<LobByteChannel> mCloseAction;
	private boolean mWriteMetadata;


	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption) throws IOException
	{
		this(aBlockAccessor, aHeader, aOpenOption, true, DEFAULT_LEAF_SIZE, DEFAULT_COMPRESSOR);
	}


	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption, boolean aWriteMetadata, int aLeafBlockSize, int aCompressorLevel) throws IOException
	{
		if (aHeader == null)
		{
			throw new IllegalArgumentException();
		}

		mHeader = aHeader;
		mBlockAccessor = aBlockAccessor;
		mWriteMetadata = aWriteMetadata;

		mCompressor = mHeader.get(IX_COMPRESSION, aCompressorLevel);

		if (aOpenOption == LobOpenOption.REPLACE)
		{
			delete();
		}

		int blockSize = mBlockAccessor.getBlockDevice().getBlockSize();

		mLeafBlockSize = mHeader.get(IX_BLOCK_SIZE, () -> (aLeafBlockSize + blockSize - 1) / blockSize * blockSize);
		mChunk = new byte[mLeafBlockSize];
		mLength = mHeader.get(IX_LOGICAL_SIZE, 0L);

		byte[] pointer = mHeader.getBinary(IX_POINTER);
		if (pointer != null)
		{
			BlockPointer tmp = new BlockPointer().unmarshal(pointer);

			if (tmp.getBlockType() == BlockType.LOB_NODE)
			{
				Console.println(tmp + " - HAS INDIRECT BLOCK");

				mIndirectBlockPointer = tmp;
				mIndirectBlock = ByteArrayBuffer.wrap(mBlockAccessor.readBlock(mIndirectBlockPointer));
			}
			else
			{
				Console.println(tmp + " - SINGLE POINTER");

				mIndirectBlock = ByteArrayBuffer.wrap(pointer).capacity(blockSize);
			}

			mIndirectBlock.position(0);
			for (int i = 0; i < mIndirectBlock.capacity(); i += SIZE)
			{
				Console.println("     +-- " + new BlockPointer().unmarshal(mIndirectBlock.read(new byte[SIZE])));
			}
		}
		else
		{
			mIndirectBlock = ByteArrayBuffer.alloc(blockSize);
		}

		if (aOpenOption == LobOpenOption.APPEND)
		{
			mPosition = mLength;
		}
		if (aOpenOption == LobOpenOption.APPEND || aOpenOption == LobOpenOption.WRITE)
		{
			mChunkIndex = -1; // force sync to load the block at mPosition
		}

		if (mWriteMetadata)
		{
			LocalDateTime now = LocalDateTime.now();
			getMetadata()
				.putIfAbsent(METADATA_CREATED, () -> now)
				.putIfAbsent(METADATA_MODIFIED, () -> now)
				.putIfAbsent(METADATA_LOGICAL_SIZE, () -> mLength);
		}
	}


	public LobByteChannel setCloseAction(Consumer<LobByteChannel> aCloseAction)
	{
		mCloseAction = aCloseAction;
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
			Console.println("READ  %d +%d", mPosition, total);
		}

		int posInChunk = posInChunk();

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, mLeafBlockSize - posInChunk);

			if (LOG)
			{
//				Console.println("\tAppend %d +%d", posInChunk, length);
			}

			aDst.put(mChunk, posInChunk, length);
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
			Console.println("WRITE %d +%d", mPosition, total);
		}

		int posInChunk = posInChunk();

		for (int remaining = total; remaining > 0;)
		{
			sync(false);

			int length = Math.min(remaining, mLeafBlockSize - posInChunk);

			if (LOG)
			{
//				Console.println("\tAppend %d +%d", posInChunk, length);
			}

			aSrc.get(mChunk, posInChunk, length);
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

		sync(false);

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

		if (LOG)
		{
			Console.println("CLOSE +%d", mLength);
		}

		if (mIndirectBlockPointer != null)
		{
			Console.println(mIndirectBlockPointer + " - FREE");

			Log.d("free indirect block");
			Log.inc();
			mBlockAccessor.freeBlock(mIndirectBlockPointer);
			mIndirectBlockPointer = null;
			Log.dec();
		}

		if (mLength > mLeafBlockSize)
		{
			mIndirectBlockPointer = mBlockAccessor.writeBlock(mIndirectBlock.array(), 0, mIndirectBlock.capacity(), BlockType.LOB_NODE, 1, CompressorAlgorithm.ZLE);
			mHeader.put(IX_POINTER, mIndirectBlockPointer.marshal());

			Console.println(mIndirectBlockPointer + " - WRITE");
		}
		else
		{
			mHeader.put(IX_POINTER, mIndirectBlock.position(0).read(new byte[SIZE]));
		}

		long physLength = 0;
		long allocLength = 0;
		mIndirectBlock.position(0);
		for (int i = 0; i < mIndirectBlock.capacity(); i += SIZE)
		{
			BlockPointer bp = new BlockPointer().unmarshal(mIndirectBlock.read(new byte[SIZE]));
			physLength += bp.getPhysicalSize();
			allocLength += bp.getAllocatedSize();
		}

		mHeader.put(IX_VERSION, 0).put(IX_LOGICAL_SIZE, mLength).put(IX_BLOCK_SIZE, mLeafBlockSize).put(IX_COMPRESSION, mCompressor);

		if (mWriteMetadata)
		{
			getMetadata()
				.putIfAbsent(METADATA_CREATED, LocalDateTime::now)
				.put(METADATA_MODIFIED, LocalDateTime.now())
				.put(METADATA_LOGICAL_SIZE, mLength)
				.put(METADATA_PHYSICAL_SIZE, physLength)
				.put(METADATA_ALLOCATED_SIZE, allocLength);
		}

		mClosed = true;
		mBlockAccessor = null;
		mIndirectBlock = null;
		mChunk = null;
		mHeader = null;
		mIndirectBlockPointer = null;

		if (mCloseAction != null)
		{
			mCloseAction.accept(this);
			mCloseAction = null;
		}

		Log.dec();
	}


	private void sync(boolean aFinal) throws IOException
	{
		if (LOG)
		{
//			Console.println("\tSync pos: %d, size: %d, final: %d, posChunk: %d, indexChunk: %d, mod: %d", mPosition, mLength, aFinal, posInChunk(), mPosition / mLeafBlockSize, mChunkModified);
		}

		if (posInChunk() == 0 || mChunkIndex != mPosition / mLeafBlockSize || aFinal)
		{
			if (mChunkModified)
			{
				int len = (int)Math.min(mLeafBlockSize, mLength - mLeafBlockSize * mChunkIndex);

				if (LOG)
				{
//					Console.println("\tWrite chunk %d, %d bytes", mChunkIndex, len);
				}

				if (mChunkIndex * SIZE < mIndirectBlock.capacity() && mIndirectBlock.position(mChunkIndex * SIZE).peekInt8() != BlockType.LOB_HOLE && mIndirectBlock.position(mChunkIndex * SIZE).peekInt8() != BlockType.HOLE)
				{
					BlockPointer bp = new BlockPointer().unmarshalBuffer(mIndirectBlock);
					mBlockAccessor.freeBlock(bp);
					Console.println(bp + " - FREE");
				}

				BlockPointer bp = mBlockAccessor.writeBlock(mChunk, 0, len, BlockType.LOB_LEAF, 0, mCompressor);
				if (bp.getPhysicalSize() == 0)
				{
					bp.setBlockType(BlockType.LOB_HOLE);
				}

				Console.println(bp + " - WRITE");

				mIndirectBlock.ensureCapacity(((mChunkIndex + 1) * SIZE + mLeafBlockSize - 1) / mLeafBlockSize * mLeafBlockSize);
				mIndirectBlock.position(mChunkIndex * SIZE).write(bp.marshal());
			}

			if (!aFinal)
			{
				Arrays.fill(mChunk, (byte)0);

				mChunkIndex = (int)(mPosition / mLeafBlockSize);
				mChunkModified = false;

				BlockPointer bp = mChunkIndex * SIZE >= mIndirectBlock.capacity() ? null : new BlockPointer().unmarshalBuffer(mIndirectBlock.position(mChunkIndex * SIZE));

				if (bp != null && bp.getPhysicalSize() > 0)
				{
					if (LOG)
					{
//						Console.println("\tRead chunk %d", mChunkIndex);
					}

					mBlockAccessor.readBlock(bp, mChunk);

					Console.println(bp + " - READ");
				}
			}
		}
	}


	public void delete()
	{
		mIndirectBlock.position(0);
		for (int i = 0; i < mIndirectBlock.capacity(); i += SIZE)
		{
			mBlockAccessor.freeBlock(new BlockPointer().unmarshal(mIndirectBlock.read(new byte[SIZE])));
		}

		mBlockAccessor.freeBlock(mIndirectBlockPointer);

		mLength = 0;
		mIndirectBlockPointer = null;
		mPosition = 0;
		mModified = true;
		mChunkModified = false;
		mChunkIndex = 0;
		mClosed = true;

		if (mChunk != null)
		{
			Arrays.fill(mChunk, (byte)0);
		}
	}


	public byte[] readAllBytes() throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate((int)(size() - position()));
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
		return mHeader.computeIfAbsent(IX_METADATA, () -> new Document());
	}


	@Override
	public String toString()
	{
		if (getMetadata().containsKey(METADATA_ALLOCATED_SIZE))
		{
			return Console.format("{alloc=%d, phys=%d, logic=%d, pointer=%S}", getMetadata().get(METADATA_ALLOCATED_SIZE), getMetadata().get(METADATA_PHYSICAL_SIZE), mHeader.get(IX_LOGICAL_SIZE, 0L), new BlockPointer().unmarshal(mHeader.get(IX_POINTER)));
		}
		return Console.format("{logic=%d, pointer=%S}", mHeader.get(IX_LOGICAL_SIZE, 0L), new BlockPointer().unmarshal(mHeader.get(IX_POINTER)));
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
