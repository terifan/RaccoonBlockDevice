package org.terifan.raccoon.blockdevice;

import java.util.Arrays;
import org.terifan.logging.LogStatement;
import org.terifan.logging.LogStatementProducer;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;


@LogStatementProducer
class Page
{
	private final static Logger log = Logger.getLogger();

	private LobByteChannel mChannel;

	PageState mState;
	BlockPointer mBlockPointer;
	Page[] mChildren;
	byte[] mBuffer;
	int mLevel;


	private Page(LobByteChannel aChannel)
	{
		mChannel = aChannel;
	}


	public static Page create(LobByteChannel aChannel)
	{
		Page page = new Page(aChannel);
		page.mBuffer = new byte[aChannel.mLeafSize];
		page.mState = PageState.PENDING;
		return page;
	}


	public static Page load(LobByteChannel aChannel, BlockPointer aBlockPointer)
	{
		Page page = new Page(aChannel);
		page.mLevel = aBlockPointer.getBlockLevel();
		page.mBlockPointer = aBlockPointer;
		page.mState = PageState.PERSISTED;

		if (aBlockPointer.getBlockType() == BlockType.HOLE)
		{
			page.mBuffer = new byte[page.mLevel == 0 ? aChannel.mLeafSize : aChannel.mNodeSize];
		}
		else
		{
			page.mBuffer = aChannel.getBlockAccessor().readBlock(aBlockPointer);
		}

		if (page.mLevel > 0)
		{
			page.mChildren = new Page[aChannel.mNodesPerPage];
		}

		return page;
	}


	int convertPageToChildIndex(long aPageIndex)
	{
		return (int)((aPageIndex / (long)Math.pow(mChannel.mNodesPerPage, mLevel - 1)) % mChannel.mNodesPerPage);
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

				page = Page.create(mChannel);
				page.mLevel = mLevel - 1;
				page.mState = PageState.PENDING;

				if (page.mLevel == 0)
				{
					page.mBuffer = new byte[mChannel.mLeafSize];
				}
				else
				{
					page.mChildren = new Page[mChannel.mNodesPerPage];
					page.mBuffer = new byte[mChannel.mNodeSize];
				}
			}
			else
			{
				page = Page.load(mChannel, ptr);
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
		BlockAccessor blockAccessor = mChannel.getBlockAccessor();

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
					blockAccessor.freeBlock(mBlockPointer);
					mState = PageState.PERSISTED;
					mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setBlockLevel(mLevel).setLogicalSize(mChannel.mNodeSize);
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

		blockAccessor.freeBlock(mBlockPointer);

		if (mLevel == 0)
		{
			mBlockPointer = blockAccessor.writeBlock(mBuffer, 0, mBuffer.length, BlockType.LOB_LEAF, mLevel, mChannel.mCompressor);
		}
		else
		{
			mBlockPointer = blockAccessor.writeBlock(mBuffer, 0, mBuffer.length, BlockType.LOB_NODE, mLevel, CompressorAlgorithm.ZLE);
		}

		mState = PageState.PERSISTED;

		log.d("page updated to {}", mBlockPointer);
		log.dec();

		return true;
	}
}
