package org.terifan.raccoon.blockdevice;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.logging.Level;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;


public class RaccoonDevice
{
	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGenerationFunction;
	private CipherModeFunction mCipherModeFunction;
	private CompressorAlgorithm mCompressorAlgorithm;
	private char[] mPassword;
	private int mKeyGeneratorIterationCount;
	private int mBlockSize;
	private Level mLoggingLevel;


	public RaccoonDevice()
	{
		mBlockSize = 4096;
		mKeyGeneratorIterationCount = 1_000_000;
		mEncryptionFunction = EncryptionFunction.AES;
		mCipherModeFunction = CipherModeFunction.XTS;
		mKeyGenerationFunction = KeyGenerationFunction.SHA512;
		mCompressorAlgorithm = CompressorAlgorithm.LZJB;
		mLoggingLevel = Level.FATAL;
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary folder with a random name. File will be deleted on exit.
	 */
	public ManagedBlockDevice inTemporaryFile()
	{
		return inTemporaryFile(null);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary folder with provided name. File will be deleted on exit.
	 */
	public ManagedBlockDevice inTemporaryFile(String aName)
	{
		try
		{
			Path path = Files.createTempFile(aName, ".rdb");
			path.toFile().deleteOnExit();
			return inFile(path);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException("Failed to create file in temporary folder", e);
		}
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the user home folder with provided relative path.
	 */
	public ManagedBlockDevice inUserFile(String aPath)
	{
		File dir = new File(System.getProperty("user.home"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("User diretory not found in this environment.");
		}
		Path path = new File(dir, aPath).toPath();
		try
		{
			Files.createDirectories(path.getParent());
		}
		catch (IOException e)
		{
			throw new RaccoonIOException("Failed to create path: " + path);
		}
		return inFile(path);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public ManagedBlockDevice inFile(Path aPath)
	{
		return new ManagedBlockDevice(secure(new FileBlockStorage(aPath, mBlockSize)));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public ManagedBlockDevice inFile(String aPath)
	{
		return inFile(Paths.get(aPath));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public ManagedBlockDevice inFile(File aPath)
	{
		return inFile(aPath.toPath());
	}


	/**
	 * Return a RaccoonBuilder with a target set to a low level BlockStorage.
	 */
	public ManagedBlockDevice inStorage(BlockStorage aBlockStorage)
	{
		return new ManagedBlockDevice(secure(aBlockStorage));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a BlockDevice that exists in RAM only.
	 *
	 * NOTE: changing the BlockSize will reset the block storage.
	 */
	public ManagedBlockDevice inMemory()
	{
		return new ManagedBlockDevice(secure(new MemoryBlockStorage(mBlockSize)));
	}


	private BlockStorage secure(BlockStorage aBlockStorage)
	{
		if (mPassword != null)
		{
			AccessCredentials credentials = new AccessCredentials(mPassword, mEncryptionFunction, mKeyGenerationFunction, mCipherModeFunction, mKeyGeneratorIterationCount);
			return new SecureBlockDevice(credentials, aBlockStorage);
		}

		return aBlockStorage;
	}


	public RaccoonDevice withEncryption(String aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction == null ? null : EncryptionFunction.valueOf(aEncryptionFunction.toUpperCase());
		return this;
	}


	public RaccoonDevice withEncryption(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public RaccoonDevice withKeyGeneration(String aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction == null ? null : KeyGenerationFunction.valueOf(aKeyGenerationFunction.toUpperCase());
		return this;
	}


	public RaccoonDevice withKeyGeneration(KeyGenerationFunction aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction;
		return this;
	}


	public RaccoonDevice withCipherMode(String aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction == null ? null : CipherModeFunction.valueOf(aCipherModeFunction.toUpperCase());
		return this;
	}


	public RaccoonDevice withCipherMode(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public RaccoonDevice withPassword(char[] aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.clone();
		return this;
	}


	public RaccoonDevice withPassword(byte[] aPassword)
	{
		if (aPassword == null)
		{
			mPassword = null;
		}
		else
		{
			char[] tmp = new char[aPassword.length];
			for (int i = 0; i < aPassword.length; i++)
			{
				tmp[i] = (char)aPassword[i];
			}
			mPassword = tmp;
		}
		return this;
	}


	public RaccoonDevice withPassword(String aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.toCharArray();
		return this;
	}


	public RaccoonDevice withCompressor(CompressorAlgorithm aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm;
		return this;
	}


	public RaccoonDevice withCompressor(String aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm == null ? null : CompressorAlgorithm.valueOf(aCompressorAlgorithm.toUpperCase());
		return this;
	}


	/**
	 * Sets the encryption key generator iteration count. Default is 1,000,000.
	 */
	public RaccoonDevice withKeyGenerationIterationCount(int aKeyGeneratorIterationCount)
	{
		mKeyGeneratorIterationCount = aKeyGeneratorIterationCount;
		return this;
	}


	/**
	 * Sets the size of a storage block in the block device. This value should be same or a multiple of the underlaying file systems
	 * sector size.
	 * <p>
	 * Note: only applicable when creating a new device.
	 * </p>
	 * @param aBlockSize must be power of 2, and between 512 and 65536, default is 4096.
	 */
	public RaccoonDevice withBlockSize(int aBlockSize)
	{
		if (aBlockSize < 512 || aBlockSize > 65536 || (aBlockSize & (aBlockSize - 1)) != 0)
		{
			throw new IllegalArgumentException("Illegal block size: " + aBlockSize);
		}

		mBlockSize = aBlockSize;
		return this;
	}


	public RaccoonDevice withLogging(Level aLevel)
	{
		mLoggingLevel = aLevel;
		return this;
	}
}
