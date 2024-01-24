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
import org.terifan.logging.LogStatement;
import org.terifan.logging.LogStatementProducer;
import org.terifan.logging.Logger;
import org.terifan.logging.Unit;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;

//                         ______________________node______________________
//                        /                       |                        \
//         ____________node____________          hole        ____________node____________
//        /             |              \                    /             |              \
//      hole         __node__       __node__              hole         __node__       __node__
//                  /   |    \     /   |    \                         /   |    \     /   |    \
//                leaf hole leaf leaf leaf hole                     hole leaf leaf leaf leaf leaf

public class LobByteChannel implements SeekableByteChannel
{
	private final static Logger log = Logger.getLogger();

//	public final static int DEFAULT_NODE_SIZE = 1024;
//	public final static int DEFAULT_LEAF_SIZE = 128 * 1024;
//	public final static int DEFAULT_COMPRESSOR = CompressorAlgorithm.NONE;
	private final static String IX_VERSION = "0";
	private final static String IX_LENGTH = "1";
	private final static String IX_NODE_SIZE = "2";
	private final static String IX_LEAF_SIZE = "3";
	private final static String IX_NODE_COMPRESSION = "4";
	private final static String IX_LEAF_COMPRESSION = "5";
	private final static String IX_METADATA = "6";
	private final static String IX_POINTER = "7";

	public final static String METADATA_MODIFIED = "$modified";
	public final static String METADATA_CREATED = "$created";
	public final static String METADATA_PHYSICAL_SIZE = "$physical";
	public final static String METADATA_ALLOCATED_SIZE = "$allocated";
	public final static String METADATA_LOGICAL_SIZE = "$logical";

	private Document mHeader;
	private BlockAccessor mBlockAccessor;
	private Consumer<LobByteChannel> mCloseAction;

	private boolean mClosed;

	private long mLength;
	private int mNodeSize;
	private int mLeafSize;
	private int mNodeCompressor;
	private int mLeafCompressor;
	private int mNodesPerPage;

	private Page mRoot;
	private long mPosition;


	static enum PageState
	{
		PENDING,
		PERSISTED
	}


	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption, boolean aWriteMetadata, int aNodeSize, int aLeafSize, int aNodeCompressor, int aLeafCompressor) throws IOException
	{
		this(aBlockAccessor, aHeader
			.putIfAbsent(IX_VERSION, () -> 0)
			.putIfAbsent(IX_LENGTH, () -> 0L)
			.putIfAbsent(IX_NODE_SIZE, () -> aNodeSize)
			.putIfAbsent(IX_LEAF_SIZE, () -> aLeafSize)
			.putIfAbsent(IX_NODE_COMPRESSION, () -> aNodeCompressor)
			.putIfAbsent(IX_LEAF_COMPRESSION, () -> aLeafCompressor)
			.putIfAbsent(IX_METADATA, () -> aWriteMetadata ? new Document() : null),
			aOpenOption);
	}


	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption) throws IOException
	{
		if (aHeader == null)
		{
			throw new IllegalArgumentException();
		}

		mHeader = aHeader;
		mBlockAccessor = aBlockAccessor;
		mLength = mHeader.computeIfAbsent(IX_LENGTH, () -> 0L);
		mNodeSize = mHeader.computeIfAbsent(IX_NODE_SIZE, () -> Math.max(aBlockAccessor.getBlockDevice().getBlockSize(), 8192));
		mLeafSize = mHeader.computeIfAbsent(IX_LEAF_SIZE, () -> Math.max(aBlockAccessor.getBlockDevice().getBlockSize(), 128 * 1024));
		mNodeCompressor = mHeader.computeIfAbsent(IX_NODE_COMPRESSION, () -> CompressorAlgorithm.ZLE);
		mLeafCompressor = mHeader.computeIfAbsent(IX_LEAF_COMPRESSION, () -> CompressorAlgorithm.ZLE);

		int blockSize = mBlockAccessor.getBlockDevice().getBlockSize();
		if (mNodeSize < blockSize || mLeafSize < blockSize)
		{
			throw new IllegalArgumentException(mNodeSize + " < " + blockSize + " || " + mLeafSize + " < " + blockSize);
		}

		mNodesPerPage = mNodeSize / BlockPointer.SIZE;

		if (aOpenOption == LobOpenOption.REPLACE)
		{
			delete();
		}

		byte[] pointer = mHeader.getBinary(IX_POINTER);
		if (pointer != null)
		{
			mRoot = new Page(new BlockPointer().unmarshal(pointer));
		}
		else
		{
			mRoot = new Page();
			mRoot.mLevel = 0;
			mRoot.mBuffer = new byte[mLeafSize];
			mRoot.mState = PageState.PENDING;
		}

		if (aOpenOption == LobOpenOption.APPEND)
		{
			mPosition = mLength;
		}

		Document metadata = getMetadata();
		if (metadata != null)
		{
			metadata
				.putIfAbsent(METADATA_CREATED, () -> LocalDateTime.now())
				.putIfAbsent(METADATA_MODIFIED, () -> LocalDateTime.now())
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

		for (long pageIndex = mPosition / mLeafSize; aDst.remaining() > 0; pageIndex++)
		{
			int pos = posOnPage();
			int len = Math.min(mLeafSize - pos, aDst.remaining());

			Page page = mRoot;
			while (page != null && page.mLevel > 0)
			{
				page = page.getChild(page.convertPageToChildIndex(pageIndex), false);
			}

			if (page == null)
			{
				aDst.put(new byte[len], 0, len);
			}
			else
			{
				aDst.put(page.mBuffer, pos, len);
			}

			mPosition += len;
			mLength = Math.max(mLength, mPosition);
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

		int length = aSrc.remaining();

		ensureCapacity(mPosition + length);

		for (long pageIndex = mPosition / mLeafSize; aSrc.remaining() > 0; pageIndex++)
		{
			int pos = posOnPage();
			int len = Math.min(mLeafSize - pos, aSrc.remaining());

			Page page = mRoot;
			while (page.mLevel > 0)
			{
				page.mState = PageState.PENDING;
				page = page.getChild(page.convertPageToChildIndex(pageIndex), true);
			}

			aSrc.get(page.mBuffer, pos, len);

			page.mState = PageState.PENDING;

			mPosition += len;
			mLength = Math.max(mLength, mPosition);
		}

		return length;
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
		if (mClosed)
		{
			return;
		}

		log.d("closing lob");
		log.inc();

		flush();

		mHeader
			.put(IX_POINTER, mRoot.mBlockPointer.marshal())
			.put(IX_VERSION, 0)
			.put(IX_LENGTH, mLength)
			.put(IX_NODE_SIZE, mNodeSize)
			.put(IX_LEAF_SIZE, mLeafSize)
			.put(IX_NODE_COMPRESSION, mNodeCompressor)
			.put(IX_LEAF_COMPRESSION, mLeafCompressor);

		Document metadata = getMetadata();
		if (metadata != null)
		{
			metadata
				.putIfAbsent(METADATA_CREATED, LocalDateTime::now)
				.put(METADATA_MODIFIED, LocalDateTime.now())
				.put(METADATA_LOGICAL_SIZE, mLength) //				.put(METADATA_PHYSICAL_SIZE, physLength)
				//				.put(METADATA_ALLOCATED_SIZE, allocLength)
				;
		}

		mClosed = true;
		mBlockAccessor = null;
		mHeader = null;

		if (mCloseAction != null)
		{
			mCloseAction.accept(this);
			mCloseAction = null;
		}

		log.dec();
	}


	public void flush()
	{
		if (mClosed)
		{
			return;
		}

		if (mRoot.mState == PageState.PENDING)
		{
			log.d("flusing lob");
			log.inc();

			mRoot.flush();

			log.dec();
		}
	}


	public void delete()
	{
//		mIndirectBlock.position(0);
//		for (int i = 0; i < mIndirectBlock.capacity(); i += SIZE)
//		{
//			mBlockAccessor.freeBlock(new BlockPointer().unmarshal(mIndirectBlock.read(new byte[SIZE])));
//		}
//
//		mBlockAccessor.freeBlock(mIndirectBlockPointer);
//
//		mLength = 0;
//		mIndirectBlockPointer = null;
//		mPosition = 0;
//		mModified = true;
//		mChunkModified = false;
//		mChunkIndex = 0;
//		mClosed = true;
//
//		if (mChunk != null)
//		{
//			Arrays.fill(mChunk, (byte)0);
//		}
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


	private int posOnPage()
	{
		return (int)(mPosition & (mLeafSize - 1));
	}


	/**
	 * @return the metadata Document with information that is stored along with the LOB. Fields with "$" prefix will be updated by the
	 * implementation.
	 */
	public Document getMetadata()
	{
		return mHeader.getDocument(IX_METADATA);
	}


	@Override
	public String toString()
	{
		Document doc = getMetadata();
		if (doc != null && doc.containsKey(METADATA_ALLOCATED_SIZE))
		{
			return String.format("{alloc=%d, phys=%d, logic=%d, pointer=%S}", doc.get(METADATA_ALLOCATED_SIZE), doc.get(METADATA_PHYSICAL_SIZE), mHeader.get(IX_LENGTH, 0L), new BlockPointer().unmarshal(mHeader.get(IX_POINTER)));
		}
		return String.format("{logic=%d, pointer=%S}", mHeader.get(IX_LENGTH, 0L), new BlockPointer().unmarshal(mHeader.get(IX_POINTER)));
	}


	private void ensureCapacity(long aCapacityBytes)
	{
		long totalPages = (aCapacityBytes + mLeafSize - 1) / mLeafSize;

		while (totalPages > Math.pow(mNodesPerPage, mRoot.mLevel))
		{
			log.d("growing tree");

			Page newRoot = new Page();
			newRoot.mState = PageState.PENDING;
			newRoot.mLevel = mRoot.mLevel + 1;
			newRoot.mChildren = new Page[mNodesPerPage];
			newRoot.mBuffer = new byte[mNodeSize];
			newRoot.mChildren[0] = mRoot;
			mRoot = newRoot;
		}
	}


	public void scan()
	{
		System.out.println("root: " + mRoot.log());

		if (mRoot.mLevel > 0)
		{
			scan(mRoot, mRoot.mLevel - 1);
		}
	}


	private void scan(Page aPage, int aLevel)
	{
		try (Unit _u = log.enter())
		{
			for (int i = 0; i < mNodesPerPage; i++)
			{
				Page child = aPage.getChild(i, false);

				if (child != null)
				{
					System.out.println("... ".repeat(mRoot.mLevel - child.mLevel) + (child.mLevel==0?"leaf: ":"node: ") + child.log());

					assert aLevel == child.mLevel;

					if (aLevel >= 1)
					{
						scan(child, aLevel - 1);
					}
				}
				else
				{
					System.out.println("... ".repeat(mRoot.mLevel - aLevel) + "hole: level=" + aLevel);
				}
			}
		}
	}


	@LogStatementProducer
	class Page
	{
		PageState mState;
		BlockPointer mBlockPointer;
		Page[] mChildren;
		byte[] mBuffer;
		int mLevel;


		public Page()
		{
		}


		public Page(BlockPointer aBlockPointer)
		{
			mBlockPointer = aBlockPointer;
			mState = PageState.PERSISTED;
			mLevel = mBlockPointer.getBlockLevel();

			if (mBlockPointer.getBlockType() == BlockType.HOLE)
			{
				mBuffer = new byte[mLevel == 0 ? mLeafSize : mNodeSize];
			}
			else
			{
				mBuffer = mBlockAccessor.readBlock(mBlockPointer);
			}

			if (mLevel > 0)
			{
				mChildren = new Page[mNodesPerPage];
			}
		}


		int convertPageToChildIndex(long aPageIndex)
		{
			return (int)((aPageIndex / (long)Math.pow(mNodesPerPage, mLevel - 1)) % mNodesPerPage);
		}


		Page getChild(int aChildIndex, boolean aCreate)
		{
			assert mLevel > 0;

			Page page = mChildren[aChildIndex];

			if (page == null)
			{
				BlockPointer ptr = getBlockPointer(aChildIndex);

				if (ptr == null || ptr.getBlockType() == BlockType.HOLE)
				{
					if (!aCreate)
					{
						return null;
					}

					page = new Page();
					page.mLevel = mLevel - 1;
					page.mState = PageState.PENDING;

					if (page.mLevel == 0)
					{
						page.mBuffer = new byte[mLeafSize];
					}
					else
					{
						page.mChildren = new Page[mNodesPerPage];
						page.mBuffer = new byte[mNodeSize];
					}
				}
				else
				{
					page = new Page(ptr);
				}

				mChildren[aChildIndex] = page;

				assert page.mLevel == mLevel - 1 : page.mLevel + " != " + (mLevel - 1) + ", index: " + aChildIndex;
			}

			return page;
		}


		BlockPointer getBlockPointer(int aChildIndex)
		{
			assert mLevel > 0;

			int offset = BlockPointer.SIZE * aChildIndex;

			if (mBlockPointer != null && mBlockPointer.getBlockType() == BlockType.HOLE)
			{
				return null;
			}

			return new BlockPointer().unmarshal(Arrays.copyOfRange(mBuffer, offset, offset + BlockPointer.SIZE));
		}


		@LogStatementProducer
		public LogStatement log()
		{
			return new LogStatement("level={}, state={}, buffer={}, ptr={}", mLevel, mState, mBuffer == null ? null : mBuffer.length, mBlockPointer);
		}


		boolean flush()
		{
			if (mLevel > 0 && mState == PageState.PENDING)
			{
				boolean detectedData = false;

				for (int i = 0; i < mChildren.length; i++)
				{
					Page child = mChildren[i];

					if (child != null && child.flush())
					{
						if (child.mBlockPointer != null)
						{
							System.arraycopy(child.mBlockPointer.marshal(), 0, mBuffer, i * BlockPointer.SIZE, BlockPointer.SIZE);
							detectedData |= child.mBlockPointer.getBlockType() != BlockType.HOLE;
						}
						else
						{
							Arrays.fill(mBuffer, i * BlockPointer.SIZE, (i + 1) * BlockPointer.SIZE, (byte)0);
						}
					}
				}

				if (!detectedData)
				{
					boolean isHole = true;
					for (int i = 0; i < mChildren.length; i++)
					{
						Page child = getChild(i, false);
						if (child != null && child.mBlockPointer != null && child.mBlockPointer.getBlockType() != BlockType.HOLE)
						{
							isHole = false;
							break;
						}
					}
					if (isHole)
					{
						mBlockAccessor.freeBlock(mBlockPointer);
						mState = PageState.PERSISTED;
						mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setBlockLevel(mLevel).setLogicalSize(mNodeSize);
						return true;
					}
				}
			}

			if (mState == PageState.PERSISTED)
			{
				log.d("skipping clean page {}", mBlockPointer);
				return false;
			}

			log.d("flushing page {}", this);
			log.inc();

			mBlockAccessor.freeBlock(mBlockPointer);

			if (mLevel == 0)
			{
				mBlockPointer = mBlockAccessor.writeBlock(mBuffer, 0, mBuffer.length, BlockType.LOB_LEAF, mLevel, mLeafCompressor);
			}
			else
			{
				mBlockPointer = mBlockAccessor.writeBlock(mBuffer, 0, mBuffer.length, BlockType.LOB_NODE, mLevel, mNodeCompressor);
			}

			mState = PageState.PERSISTED;

			log.d("page updated to {}", mBlockPointer);
			log.dec();

			return true;
		}
	}
}
