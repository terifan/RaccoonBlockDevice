package org.terifan.raccoon.blockdevice.compressor;


public enum CompressorAlgorithm
{
	NONE,
	ZLE,
	LZJB,
	DEFLATE_FAST,
	DEFLATE_DEFAULT,
	DEFLATE_BEST;


	public static boolean compress(int aAlgorithm, byte[] aInput, int aOffset, int aLength, ByteBlockOutputStream aOutputStream)
	{
		switch (aAlgorithm)
		{
			case 1:
				return new ZLE().compress(aInput, aOffset, aLength, aOutputStream);
			case 2:
				return new LZJB().compress(aInput, aOffset, aLength, aOutputStream);
			case 3:
				return new DeflateFast().compress(aInput, aOffset, aLength, aOutputStream);
			case 4:
				return new DeflateDefault().compress(aInput, aOffset, aLength, aOutputStream);
			case 5:
				return new DeflateBest().compress(aInput, aOffset, aLength, aOutputStream);
		}
		throw new IllegalArgumentException("Unsupported compression algorithm: " + aAlgorithm);
	}


	public static boolean decompress(int aAlgorithm, byte[] aInput, int aInputLength, byte[] aOutput, int aOutputLength)
	{
		switch (aAlgorithm)
		{
			case 0:
				System.arraycopy(aInput, 0, aOutput, 0, aOutputLength);
				return true;
			case 1:
				return new ZLE().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case 2:
				return new LZJB().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case 3:
				return new DeflateFast().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case 4:
				return new DeflateDefault().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case 5:
				return new DeflateBest().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
		}
		throw new IllegalArgumentException("Unsupported compression algorithm: " + aAlgorithm);
	}
}
