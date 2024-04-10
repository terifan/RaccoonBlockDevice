package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.security.messagedigest.Fletcher4;
import org.terifan.raccoon.security.messagedigest.MurmurHash3;
import org.terifan.raccoon.security.messagedigest.SHA3;
import org.terifan.raccoon.security.messagedigest.SHA512;
import org.terifan.raccoon.security.messagedigest.Skein512;


public interface ChecksumAlgorithm
{
	int FLETCHER4 = 0;
	int MURMURHASH3 = 1;
	int SKEIN512 = 2;
	int SHA2_512 = 3;
	int SHA3_256 = 4;
	int SHA3_512 = 5;

	static int[] hash128(int aAlgorithm, byte[] aData, int aOffset, int aLength, long aSeed)
	{
		switch (aAlgorithm)
		{
			case MURMURHASH3:
				return MurmurHash3.hash128(aData, aOffset, aLength, aSeed);
			case FLETCHER4:
				return Fletcher4.hash128(aData, aOffset, aLength, aSeed);
			case SKEIN512:
				return Skein512.hash128(aData, aOffset, aLength, aSeed);
			case SHA2_512:
				return SHA512.hash128(aData, aOffset, aLength, aSeed);
			case SHA3_256:
				return SHA3.hash128_256(aData, aOffset, aLength, aSeed);
			case SHA3_512:
				return SHA3.hash128_512(aData, aOffset, aLength, aSeed);
		}
		throw new IllegalArgumentException("Unsupported checksum algorithm: " + aAlgorithm);
	}
}
