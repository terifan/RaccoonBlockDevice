package org.terifan.raccoon.blockdevice;

import java.io.IOException;


interface Compressor
{
	boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteBlockOutputStream aOutputStream);

	void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException;
}
