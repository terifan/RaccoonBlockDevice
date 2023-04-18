package org.terifan.raccoon.blockdevice.compressor;


public enum CompressorLevel
{
	NONE,
	ZLE,
	LZJB,
	DEFLATE_FAST,
	DEFLATE_DEFAULT,
	DEFLATE_BEST;


	public Compressor instance()
	{
		switch (this)
		{
			case NONE:
				return null;
			case ZLE:
				return new ZLE();
			case LZJB:
				return new LZJB();
			case DEFLATE_BEST:
				return new DeflateBest();
			case DEFLATE_DEFAULT:
				return new DeflateDefault();
			case DEFLATE_FAST:
				return new DeflateFast();
			default:
				throw new IllegalStateException();
		}
	}
}
