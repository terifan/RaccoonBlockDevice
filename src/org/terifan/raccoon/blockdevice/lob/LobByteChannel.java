package org.terifan.raccoon.blockdevice.lob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.function.Consumer;
import org.terifan.logging.Logger;
import org.terifan.logging.Unit;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
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

	private final static String IX_VERSION = "0";
	private final static String IX_LENGTH = "1";
	private final static String IX_NODES_PER_PAGE = "2";
	private final static String IX_LEAF_SIZE = "3";
	private final static String IX_COMPRESSION = "4";
	private final static String IX_POINTER = "5";
	private final static String IX_METADATA = "6";

	private BlockAccessor mBlockAccessor;
	private Consumer<LobByteChannel> mCloseAction;
	private Document mHeader;
	private boolean mClosed;
	private long mLength;
	private LobPage mRoot;
	private long mPosition;

	int mNodeSize;
	int mLeafSize;
	int mCompressor;
	int mNodesPerPage;


	/**
	 * Create or open a LobByteChannel
	 *
	 * @param aOptions a Document with options for creating the LobByteChannel
	 * <li><b>node</b> - Number of pointers in an interior node. Integer. Default 128.
	 * <li><b>leaf</b> - Size of a leaf node. Integer. Default 131072.
	 * <li><b>compression</b> - Name of a compressor used to compress the leaf nodes. String. See {@link org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm}. Default lzjb.
	 */
	public LobByteChannel(BlockAccessor aBlockAccessor, Document aHeader, LobOpenOption aOpenOption, Document aOptions) throws IOException
	{
		if (aHeader == null)
		{
			throw new IllegalArgumentException();
		}

		mHeader = aHeader;
		mBlockAccessor = aBlockAccessor;
		mNodesPerPage = mHeader.computeIfAbsent(IX_NODES_PER_PAGE, k -> 128);
		mLeafSize = mHeader.computeIfAbsent(IX_LEAF_SIZE, k -> Math.max(aBlockAccessor.getBlockDevice().getBlockSize(), 128 * 1024));
		mCompressor = mHeader.computeIfAbsent(IX_COMPRESSION, k -> CompressorAlgorithm.LZJB.ordinal());
		mLength = mHeader.computeIfAbsent(IX_LENGTH, k -> 0L);

		mNodeSize = mHeader.getInt(IX_NODES_PER_PAGE) * BlockPointer.SIZE;

		assert mNodeSize >= mBlockAccessor.getBlockDevice().getBlockSize();
		assert mLeafSize >= mBlockAccessor.getBlockDevice().getBlockSize();

		if (aOpenOption == LobOpenOption.REPLACE)
		{
			delete();
		}

		byte[] pointer = mHeader.getBinary(IX_POINTER);
		if (pointer != null)
		{
			mRoot = LobPage.load(this, new BlockPointer().unmarshal(pointer));
		}
		else
		{
			mRoot = LobPage.create(this);
		}

		if (aOpenOption == LobOpenOption.APPEND)
		{
			mPosition = mLength;
		}
	}


	public LobByteChannel setCloseAction(Consumer<LobByteChannel> aCloseAction)
	{
		mCloseAction = aCloseAction;
		return this;
	}


	@Override
	public synchronized int read(ByteBuffer aDst) throws IOException
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

			LobPage page = mRoot;
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
	public synchronized int write(ByteBuffer aSrc) throws IOException
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

			LobPage page = mRoot;
			while (page.mLevel > 0)
			{
				page.mState = LobPageState.PENDING;
				page = page.getChild(page.convertPageToChildIndex(pageIndex), true);
			}

			aSrc.get(page.mBuffer, pos, len);

			page.mState = LobPageState.PENDING;

			mPosition += len;
			mLength = Math.max(mLength, mPosition);
		}

		return length;
	}


	@Override
	public synchronized long position() throws IOException
	{
		return mPosition;
	}


	@Override
	public synchronized SeekableByteChannel position(long aNewPosition) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		mPosition = aNewPosition;
		return this;
	}


	@Override
	public synchronized long size() throws IOException
	{
		return mLength;
	}


	@Override
	public synchronized SeekableByteChannel truncate(long aSize) throws IOException
	{
		if (mClosed)
		{
			throw new ClosedChannelException();
		}

		throw new UnsupportedOperationException();
	}


	@Override
	public synchronized boolean isOpen()
	{
		return !mClosed;
	}


	@Override
	public synchronized void close() throws IOException
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
			.put(IX_NODES_PER_PAGE, mNodesPerPage)
			.put(IX_LEAF_SIZE, mLeafSize)
			.put(IX_COMPRESSION, mCompressor);

		mClosed = true;
		mHeader = null;
		mBlockAccessor = null;

		if (mCloseAction != null)
		{
			mCloseAction.accept(this);
			mCloseAction = null;
		}

		log.dec();
	}


	public synchronized void flush()
	{
		if (mClosed)
		{
			return;
		}

		if (mRoot.mState == LobPageState.PENDING)
		{
			log.d("flusing lob");
			log.inc();

			mRoot.flush();

			log.dec();
		}
	}


	public synchronized void delete()
	{
		throw new UnsupportedOperationException();
	}


	public synchronized byte[] readAllBytes() throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate((int)(size() - position()));
		read(buf);
		return buf.array();
	}


	public synchronized byte[] readAllBytes(byte[] aBuffer) throws IOException
	{
		ByteBuffer buf = ByteBuffer.wrap(aBuffer);
		read(buf);
		return aBuffer;
	}


	public synchronized LobByteChannel readAllBytes(OutputStream aDst) throws IOException
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


	public synchronized LobByteChannel writeAllBytes(byte[] aSrc) throws IOException
	{
		write(ByteBuffer.wrap(aSrc));
		return this;
	}


	public synchronized LobByteChannel writeAllBytes(InputStream aSrc) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(1024);

		for (int len; (len = aSrc.read(buf.array())) > 0;)
		{
			buf.position(0).limit(len);
			write(buf);
		}

		return this;
	}


	public synchronized InputStream newInputStream()
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


	public synchronized OutputStream newOutputStream()
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


	public BlockAccessor getBlockAccessor()
	{
		return mBlockAccessor;
	}


	public Document getMetadata()
	{
		return mHeader.computeIfAbsent(IX_METADATA, k -> new Document());
	}


	public LobByteChannel setMetadata(Document aDocument)
	{
		mHeader.put(IX_METADATA, aDocument);
		return this;
	}


	@Override
	public String toString()
	{
		return String.format("{logic=%d, pointer=%S}", mHeader.get(IX_LENGTH, 0L), new BlockPointer().unmarshal(mHeader.get(IX_POINTER)));
	}


	private void ensureCapacity(long aCapacityBytes)
	{
		long totalPages = (aCapacityBytes + mLeafSize - 1) / mLeafSize;

		while (totalPages > Math.pow(mNodesPerPage, mRoot.mLevel))
		{
			log.d("growing tree");

			LobPage newRoot = LobPage.create(this);
			newRoot.mState = LobPageState.PENDING;
			newRoot.mLevel = mRoot.mLevel + 1;
			newRoot.mChildren = new LobPage[mNodesPerPage];
			newRoot.mBuffer = new byte[mNodeSize];
			newRoot.mChildren[0] = mRoot;
			mRoot = newRoot;
		}
	}


	public synchronized void scan()
	{
		System.out.println("root: " + mRoot.log());

		if (mRoot.mLevel > 0)
		{
			scan(mRoot, mRoot.mLevel - 1);
		}
	}


	private void scan(LobPage aPage, int aLevel)
	{
		try (Unit _u = log.enter())
		{
			for (int i = 0; i < mNodesPerPage; i++)
			{
				LobPage child = aPage.getChild(i, false);

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
}
