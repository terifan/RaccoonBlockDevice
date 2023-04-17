package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.blockdevice.BlockType.BLOB_INDEX;
import static org.terifan.raccoon.blockdevice.BlockType.BLOB_LEAF;
import static org.terifan.raccoon.blockdevice.CompressionParam.Level.DEFLATE_BEST;
import static org.terifan.raccoon.blockdevice.CompressionParam.Level.DEFLATE_DEFAULT;
import static org.terifan.raccoon.blockdevice.CompressionParam.Level.DEFLATE_FAST;
import static org.terifan.raccoon.blockdevice.CompressionParam.Level.NONE;
import static org.terifan.raccoon.blockdevice.CompressionParam.Level.ZLE;


public final class CompressionParam
{
	public enum Level
	{
		NONE,
		ZLE,
		DEFLATE_FAST,
		DEFLATE_DEFAULT,
		DEFLATE_BEST
	}

	public final static CompressionParam BEST_SPEED = new CompressionParam(DEFLATE_FAST, ZLE, ZLE, NONE);
	public final static CompressionParam BEST_COMPRESSION = new CompressionParam(DEFLATE_DEFAULT, DEFLATE_DEFAULT, DEFLATE_DEFAULT, DEFLATE_BEST);
	public final static CompressionParam NO_COMPRESSION = new CompressionParam(NONE, NONE, NONE, NONE);

	private Document mConfiguration;


	public CompressionParam()
	{
		this(NONE, NONE, NONE, NONE);
	}


	public CompressionParam(Level aTreeIndex, Level aTreeLeaf, Level aBlobIndex, Level aBlobLeaf)
	{
		mConfiguration = new Document()
			.put("treeLeaf", aTreeLeaf.ordinal())
			.put("treeIndex", aTreeIndex.ordinal())
			.put("blobLeaf", aBlobLeaf.ordinal())
			.put("blobIndex", aBlobIndex.ordinal());
	}


	public Level getCompressorLevel(int aType)
	{
		switch (aType)
		{
//			case TREE_INDEX:
//				return Level.values()[mConfiguration.get("treeIndex", NONE.ordinal())];
//			case TREE_LEAF:
//				return Level.values()[mConfiguration.get("treeLeaf", NONE.ordinal())];
			case BLOB_INDEX:
				return Level.values()[mConfiguration.get("blobIndex", NONE.ordinal())];
			case BLOB_LEAF:
				return Level.values()[mConfiguration.get("blobNode", NONE.ordinal())];
			default:
				return NONE;
		}
	}


	public Document marshal()
	{
		return mConfiguration;
	}


	public static CompressionParam unmarshal(Document aDocument)
	{
		CompressionParam cp = new CompressionParam();
		cp.mConfiguration = aDocument;
		return cp;
	}


	@Override
	public String toString()
	{
		return mConfiguration.toString();
	}
}
