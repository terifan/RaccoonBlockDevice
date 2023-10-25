package org.terifan.raccoon.blockdevice;


public interface BlockType
{
	int FREE = 0;
	int TREE_NODE = 1;
	int TREE_LEAF = 2;
	int LOB_NODE = 3;
	int LOB_LEAF = 4;
	int ILLEGAL = 5;
	int SPACEMAP = 6;
}
