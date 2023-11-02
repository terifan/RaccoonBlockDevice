package org.terifan.raccoon.blockdevice;


public interface BlockType
{
	int HOLE = 0;
	int SYSTEM = 1;
	int SPACEMAP = 2;
	int ILLEGAL = 3;
	int UNKNOWN = 4;
	int BTREE_NODE = 5;
	int BTREE_LEAF = 6;
	int LOB_NODE = 7;
	int LOB_LEAF = 8;
	int LOB_HOLE = 9;

	// code 128-255 indicate not a BlockPointer

	int NOT_BLOCKPOINTER = 128;


	static String lookup(int aCode)
	{
		return new String[]{"HOLE","SYSTEM","SPACEMAP","ILLEGAL","UNKNOWN","BTREENODE","BTREELEAF","LOBNODE","LOBLEAF","LOBHOLE"}[aCode];
	}
}
