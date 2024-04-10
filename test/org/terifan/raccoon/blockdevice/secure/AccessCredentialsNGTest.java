package org.terifan.raccoon.blockdevice.secure;

import java.util.HexFormat;
import static org.terifan.raccoon.blockdevice.secure.CipherModeFunction.XTS;
import static org.terifan.raccoon.blockdevice.secure.EncryptionFunction.AES;
import static org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction.SKEIN512;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class AccessCredentialsNGTest
{
	@Test
	public void testSomeMethod()
	{
		AccessCredentials ac = new AccessCredentials("password".toCharArray()).setEncryptionFunction(AES).setKeyGeneratorFunction(SKEIN512).setCipherModeFunction(XTS).setIterationCount(1024);

		byte[] salt = "salt".getBytes();
		byte[] keypool = ac.generateKeyPool(salt, 16);

		assertEquals(HexFormat.of().formatHex(keypool).toUpperCase(), "0B875E667E38918FF04F8F1737790836");
	}
}
