package org.terifan.raccoon.io;


public interface BlockType
{
	static int FREE = 0;
	static int SPACEMAP = 1;
	static int BLOB_INDEX = 2;
	static int BLOB_LEAF = 3;
	static int HOLE = 4;
}
