package org.terifan.raccoon.blockdevice.compressor;

import java.util.zip.Deflater;


public class DeflateBest extends DeflateCompressor
{
	public DeflateBest()
	{
		super(Deflater.BEST_COMPRESSION);
	}
}
