package org.terifan.raccoon.blockdevice.compressor;


public interface CompressorAlgorithm
{
	int NONE = 0;
	int ZLE = 1;
	int LZJB = 2;
	int DEFLATE_FAST = 3;
	int DEFLATE_DEFAULT = 4;
	int DEFLATE_BEST = 5;


	static boolean compress(int aAlgorithm, byte[] aInput, int aOffset, int aLength, ByteBlockOutputStream aOutputStream)
	{
		switch (aAlgorithm)
		{
			case NONE:
				throw new IllegalArgumentException("Illegal value");
			case ZLE:
				return new ZLE().compress(aInput, aOffset, aLength, aOutputStream);
			case LZJB:
				return new LZJB().compress(aInput, aOffset, aLength, aOutputStream);
			case DEFLATE_BEST:
				return new DeflateBest().compress(aInput, aOffset, aLength, aOutputStream);
			case DEFLATE_DEFAULT:
				return new DeflateDefault().compress(aInput, aOffset, aLength, aOutputStream);
			case DEFLATE_FAST:
				return new DeflateFast().compress(aInput, aOffset, aLength, aOutputStream);
		}
		throw new IllegalArgumentException("Unsupported compression algorithm: " + aAlgorithm);
	}


	static boolean decompress(int aAlgorithm, byte[] aBuffer, int aInputLength, int aOutputLength)
	{
		if (aAlgorithm == NONE)
		{
			return true;
		}
		byte[] tmp = new byte[aOutputLength];
		if (decompressImpl(aAlgorithm, aBuffer, aInputLength, tmp, aOutputLength))
		{
			System.arraycopy(tmp, 0, aBuffer, 0, tmp.length);
			return true;
		}
		return false;
	}

	static boolean decompressImpl(int aAlgorithm, byte[] aInput, int aInputLength, byte[] aOutput, int aOutputLength)
	{
		switch (aAlgorithm)
		{
			case ZLE:
				return new ZLE().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case LZJB:
				return new LZJB().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case DEFLATE_BEST:
				return new DeflateBest().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case DEFLATE_DEFAULT:
				return new DeflateDefault().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			case DEFLATE_FAST:
				return new DeflateFast().decompress(aInput, 0, aInputLength, aOutput, 0, aOutputLength);
			default:
				throw new IllegalArgumentException("Unsupported compression algorithm: " + aAlgorithm);
		}
	}
}
