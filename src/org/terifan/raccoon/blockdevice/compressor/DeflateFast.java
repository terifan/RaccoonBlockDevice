package org.terifan.raccoon.blockdevice.compressor;

import java.util.zip.Deflater;


public class DeflateFast extends DeflateCompressor
{
	public DeflateFast()
	{
		super(Deflater.BEST_SPEED);
	}
}
