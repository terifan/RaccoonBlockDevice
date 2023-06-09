package org.terifan.raccoon.blockdevice.secure;

import java.security.MessageDigest;
import org.terifan.raccoon.security.messagedigest.SHA3;
import org.terifan.raccoon.security.messagedigest.SHA512;
import org.terifan.raccoon.security.messagedigest.Skein512;


public enum KeyGenerationFunction
{
	SHA512,
	SKEIN512,
	SHA3;


	MessageDigest newInstance()
	{
		switch (this)
		{
			case SHA512:
				return new SHA512();
			case SKEIN512:
				return new Skein512();
			case SHA3:
				return new SHA3(512);
		}

		throw new IllegalStateException();
	}
}