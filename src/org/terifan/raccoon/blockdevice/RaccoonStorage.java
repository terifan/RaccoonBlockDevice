package org.terifan.raccoon.blockdevice;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.logging.Level;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import static org.terifan.raccoon.blockdevice.util.PathUtils.produceAppDataPath;
import static org.terifan.raccoon.blockdevice.util.PathUtils.produceTemporaryPath;
import static org.terifan.raccoon.blockdevice.util.PathUtils.produceUserHomePath;


public class RaccoonStorage
{
	private CompressorAlgorithm mCompressorAlgorithm;
	private AccessCredentials mAccessCredentials;
	private int mBlockSize;
	private Level mLoggingLevel;
	private boolean mSecure;


	public RaccoonStorage()
	{
		mBlockSize = 4096;
		mAccessCredentials = new AccessCredentials(null);
		mCompressorAlgorithm = CompressorAlgorithm.LZJB;
		mLoggingLevel = Level.FATAL;
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary directory with a random name. File will be deleted on exit.
	 *
	 * @see org.terifan.raccoon.blockdevice.util.PathUtils
	 */
	public RaccoonStorageInstance inTemporaryFile()
	{
		return inTemporaryFile(null);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary directory with provided name. File will be deleted on exit.
	 *
	 * @see org.terifan.raccoon.blockdevice.util.PathUtils
	 */
	public RaccoonStorageInstance inTemporaryFile(String aName)
	{
		return inFile(produceTemporaryPath(aName));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the user home directory with provided relative path.
	 *
	 * @see org.terifan.raccoon.blockdevice.util.PathUtils
	 */
	public RaccoonStorageInstance inUserHome(String aPath)
	{
		return inFile(produceUserHomePath(aPath));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public RaccoonStorageInstance inFile(Path aPath)
	{
		return new RaccoonStorageInstance(secure(new FileBlockStorage(aPath, mBlockSize)));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public RaccoonStorageInstance inFile(String aPath)
	{
		return inFile(Paths.get(aPath));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 *
	 * @see org.terifan.raccoon.blockdevice.util.PathUtils
	 */
	public RaccoonStorageInstance inAppData(String aAppName, String aPath)
	{
		Path path = produceAppDataPath(aAppName, aPath);

		if (path != null)
		{
			return inFile(path);
		}

		return inUserHome(String.join("/", aAppName, aPath));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public RaccoonStorageInstance inFile(File aPath)
	{
		return inFile(aPath.toPath());
	}


	/**
	 * Return a RaccoonBuilder with a target set to a low level BlockStorage.
	 */
	public RaccoonStorageInstance inStorage(BlockStorage aBlockStorage)
	{
		return new RaccoonStorageInstance(secure(aBlockStorage));
	}


	/**
	 * Return a RaccoonBuilder with a target set to a BlockDevice that exists in RAM only.
	 */
	public RaccoonStorageInstance inMemory()
	{
		return new RaccoonStorageInstance(secure(new MemoryBlockStorage(mBlockSize)));
	}


	private BlockStorage secure(BlockStorage aBlockStorage)
	{
		if (mSecure)
		{
			return new SecureBlockDevice(mAccessCredentials, aBlockStorage);
		}

		return aBlockStorage;
	}


	/**
	 * Specifies the encryption function used.
	 * <p>
	 * Note: this value only need to be specified when creating a new storage.
	 */
	public RaccoonStorage withEncryption(EncryptionFunction aEncryptionFunction)
	{
		mAccessCredentials.setEncryptionFunction(aEncryptionFunction);
		return this;
	}


	/**
	 * Specifies the key generator function used.
	 * <p>
	 * Note: this value only need to be specified when creating a new storage.
	 */
	public RaccoonStorage withKeyGeneration(KeyGenerationFunction aKeyGenerationFunction)
	{
		mAccessCredentials.setKeyGeneratorFunction(aKeyGenerationFunction);
		return this;
	}


	/**
	 * Specifies the cipher mode used.
	 * <p>
	 * Note: this value only need to be specified when creating a new storage.
	 */
	public RaccoonStorage withCipherMode(CipherModeFunction aCipherModeFunction)
	{
		mAccessCredentials.setCipherModeFunction(aCipherModeFunction);
		return this;
	}


	public RaccoonStorage withPassword(Object aPassword)
	{
		mSecure = aPassword != null;
		if (mSecure)
		{
			mAccessCredentials.setPassword(aPassword);
		}
		return this;
	}


	public RaccoonStorage withCompressor(CompressorAlgorithm aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm;
		return this;
	}


	public RaccoonStorage withCompressor(String aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm == null ? null : CompressorAlgorithm.valueOf(aCompressorAlgorithm.toUpperCase());
		return this;
	}


	/**
	 * Sets the encryption key generator cost. These settings aren't stored thus must be specified when opening an encrypted storage.
	 *
	 * @see org.terifan.raccoon.security.messagedigest.SCrypt
	 */
	public RaccoonStorage withKeyGenerationCost(int aCost, int aRounds, int aParallelization, int aIterations)
	{
		mAccessCredentials.setCost(aCost);
		mAccessCredentials.setRounds(aRounds);
		mAccessCredentials.setParallelization(aParallelization);
		mAccessCredentials.setIterationCount(aIterations);
		return this;
	}


	/**
	 * Sets the size of a storage block in the block device. This value should be same or a multiple of the underlaying file systems sector
	 * size.
	 * <p>
	 * Note: only applicable when creating a new device.
	 *
	 * @param aBlockSize must be power of 2, and between 512 and 65536, default is 4096.
	 */
	public RaccoonStorage withBlockSize(int aBlockSize)
	{
		if (aBlockSize < 512 || aBlockSize > 65536 || (aBlockSize & (aBlockSize - 1)) != 0)
		{
			throw new IllegalArgumentException("Illegal block size: " + aBlockSize);
		}

		mBlockSize = aBlockSize;
		return this;
	}


	public RaccoonStorage withLogging(Level aLevel)
	{
		mLoggingLevel = aLevel;
		return this;
	}
}
