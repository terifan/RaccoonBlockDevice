package org.terifan.raccoon.blockdevice.compressor;

import java.util.zip.Deflater;


public class DeflateDefault extends DeflateCompressor
{
	public DeflateDefault()
	{
		super(Deflater.DEFAULT_COMPRESSION);
	}
}
