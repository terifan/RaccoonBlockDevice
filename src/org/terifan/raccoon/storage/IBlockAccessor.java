package org.terifan.raccoon.storage;


public interface IBlockAccessor
{
	void freeBlock(BlockPointer aBlockPointer);


	byte[] readBlock(BlockPointer aBlockPointer);


	BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, int aType);
}