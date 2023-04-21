package org.terifan.raccoon.blockdevice.secure;

import org.terifan.raccoon.security.cryptography.ciphermode.CBCCipherMode;
import org.terifan.raccoon.security.cryptography.ciphermode.CipherMode;
import org.terifan.raccoon.security.cryptography.ciphermode.PCBCCipherMode;
import org.terifan.raccoon.security.cryptography.ciphermode.XTSCipherMode;


public enum CipherModeFunction
{
	XTS,
	CBC,
	PCBC;


	CipherMode newInstance()
	{
		switch (this)
		{
			case CBC:
				return new CBCCipherMode();
			case PCBC:
				return new PCBCCipherMode();
			case XTS:
				return new XTSCipherMode();
		}

		throw new IllegalStateException();
	}
}