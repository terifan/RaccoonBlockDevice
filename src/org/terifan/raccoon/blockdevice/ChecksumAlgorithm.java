package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.security.messagedigest.SHA3;
import org.terifan.raccoon.security.messagedigest.SHA512;
import org.terifan.raccoon.security.messagedigest.Skein512;


public interface ChecksumAlgorithm
{
	int MurmurHash3 = 0;
	int Fletcher4 = 1;
	int Skein512 = 2;
	int SHA512 = 3;
	int SHA3 = 4;

	static int[] hash128(int aAlgorithm, byte[] aData, int aOffset, int aLength, long aSeed)
	{
		switch (aAlgorithm)
		{
			case MurmurHash3:
				return org.terifan.raccoon.security.messagedigest.MurmurHash3.hash128(aData, aOffset, aLength, aSeed);
			case Fletcher4:
				return org.terifan.raccoon.security.messagedigest.Fletcher4.hash128(aData, aOffset, aLength, aSeed);
			case Skein512:
				return new Skein512().hash128(aData, aOffset, aLength, aSeed);
			case SHA3:
				return new SHA3().hash128(aData, aOffset, aLength, aSeed);
			case SHA512:
				return new SHA512().hash128(aData, aOffset, aLength, aSeed);
		}
		throw new IllegalArgumentException("Unsupported checksum algorithm: " + aAlgorithm);
	}
}
