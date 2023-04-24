package org.terifan.raccoon.blockdevice;


public interface BlockType
{
	int FREE = 0;
	int SPACEMAP = 1;
	int LOB_INDEX = 2;
	int LOB_LEAF = 3;
	int HOLE = 4;
}
