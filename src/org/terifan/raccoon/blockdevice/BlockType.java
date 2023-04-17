package org.terifan.raccoon.blockdevice;


public interface BlockType
{
	int FREE = 0;
	int SPACEMAP = 1;
	int BLOB_INDEX = 2;
	int BLOB_LEAF = 3;
	int HOLE = 4;
}
