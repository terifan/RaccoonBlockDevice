package org.terifan.raccoon.blockdevice.secure;

import java.util.HexFormat;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class AccessCredentialsNGTest
{
	@Test
	public void testSomeMethod()
	{
		AccessCredentials ac = new AccessCredentials("password".toCharArray(), EncryptionFunction.AES, KeyGenerationFunction.SHA512, CipherModeFunction.XTS);

		byte[] salt = "salt".getBytes();
		byte[] keypool = ac.generateKeyPool(KeyGenerationFunction.SKEIN512, salt, 16);

		assertEquals(HexFormat.of().formatHex(keypool), "0B875E667E38918FF04F8F1737790836");
	}
}
