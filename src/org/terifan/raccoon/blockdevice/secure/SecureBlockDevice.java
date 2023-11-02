package org.terifan.raccoon.blockdevice.secure;

import org.terifan.raccoon.security.cryptography.BlockCipher;
import org.terifan.raccoon.security.messagedigest.MurmurHash3;
import org.terifan.raccoon.security.cryptography.SecretKey;
import static java.util.Arrays.fill;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getBytes;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt32;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.security.cryptography.ciphermode.CipherMode;
import org.terifan.raccoon.security.random.ISAAC.PRNG;
import org.terifan.raccoon.security.random.SecureRandom;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;


/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device.
 *
 * [boot block][super block 0][super block 1][other blocks]
 *
 */
public final class SecureBlockDevice implements BlockStorage, AutoCloseable
{
	private final static int SALT_SIZE = 256;
	private final static int PAYLOAD_SIZE = 256;
	private final static int HEADER_SIZE = 4;
	private final static int KEY_SIZE_BYTES = 32;
	private final static int IV_SIZE = 16;
	private final static int KEY_POOL_SIZE = KEY_SIZE_BYTES + 3 * KEY_SIZE_BYTES + 3 * IV_SIZE;
	private final static int CHECKSUM_SEED = 0xfedcba98;

	private transient int mBootBlockCount;
	private transient BlockStorage mBlockDevice;
	private transient CipherImplementation mCipherImplementation;


	public SecureBlockDevice(AccessCredentials aAccessCredentials, BlockStorage aBlockDevice) throws InvalidPasswordException
	{
		if (aBlockDevice == null)
		{
			throw new IllegalArgumentException("BlockDevice is null");
		}
		if (aAccessCredentials == null)
		{
			throw new IllegalArgumentException("AccessCredentials is null");
		}

		mBootBlockCount = 2;
		mBlockDevice = aBlockDevice;

		if (mBlockDevice.size() == 0)
		{
			Log.i("create boot block");
			Log.inc();

			for (boolean valid = false; !valid;)
			{
				valid = true;
				byte[] payload = new SecureRandom().bytes(PAYLOAD_SIZE).toArray();

				for (int index = 0; index < mBootBlockCount; index++)
				{
					byte[] blockData = createBootBlock(aAccessCredentials, payload, index, mBlockDevice.getBlockSize());

					mBlockDevice.writeBlock(index, blockData, 0, blockData.length, new int[4]);

					CipherImplementation cipher = readBootBlock(aAccessCredentials, blockData, index, true);

					if (cipher == null)
					{
						valid = false;
						break;
					}

					mCipherImplementation = cipher;
				}
			}

			Log.dec();
		}
		else
		{
			mBlockDevice = aBlockDevice;

			byte[] blockData = new byte[mBlockDevice.getBlockSize()];

			for (int index = 0; index < mBootBlockCount; index++)
			{
				Log.i("open boot block #%s", index);
				Log.inc();

				mBlockDevice.readBlock(index, blockData, 0, blockData.length, new int[4]);

				mCipherImplementation = readBootBlock(aAccessCredentials, blockData, index, false);

				if (mCipherImplementation != null)
				{
					break;
				}

				Log.dec();
			}

			if (mCipherImplementation == null)
			{
				throw new InvalidPasswordException("Incorrect password or not a secure BlockDevice");
			}
		}
	}


	private static byte[] createBootBlock(AccessCredentials aAccessCredentials, byte[] aPayload, long aBlockIndex, int aBlockSize)
	{
		byte[] salt = new byte[SALT_SIZE];
		byte[] padding = new byte[aBlockSize - SALT_SIZE - PAYLOAD_SIZE];

		// padding and salt
		PRNG.nextBytes(padding);
		PRNG.nextBytes(salt);

		// compute checksum
		int checksum = computeChecksum(salt, aPayload);

		// update header
		putInt32(aPayload, 0, checksum);

		// create user key
		byte[] userKeyPool = aAccessCredentials.generateKeyPool(aAccessCredentials.getKeyGeneratorFunction(), salt, KEY_POOL_SIZE);

		// encrypt payload
		byte[] payload = aPayload.clone();

		CipherImplementation cipher = new CipherImplementation(aAccessCredentials.getCipherModeFunction(), aAccessCredentials.getEncryptionFunction(), userKeyPool, 0, PAYLOAD_SIZE);
		cipher.encrypt(aBlockIndex, payload, 0, PAYLOAD_SIZE, new int[4]);

		// assemble output buffer
		byte[] blockData = new byte[aBlockSize];
		System.arraycopy(salt, 0, blockData, 0, SALT_SIZE);
		System.arraycopy(payload, 0, blockData, SALT_SIZE, PAYLOAD_SIZE);
		System.arraycopy(padding, 0, blockData, SALT_SIZE + PAYLOAD_SIZE, padding.length);

		return blockData;
	}


	private static CipherImplementation readBootBlock(AccessCredentials aAccessCredentials, byte[] aBlockData, long aBlockIndex, boolean aVerifyFunctions)
	{
		// extract the salt and payload
		byte[] salt = getBytes(aBlockData, 0, SALT_SIZE);
		byte[] payload = getBytes(aBlockData, SALT_SIZE, PAYLOAD_SIZE);

		for (KeyGenerationFunction keyGenerator : KeyGenerationFunction.values())
		{
			// create a user key using the key generator
			byte[] userKeyPool = aAccessCredentials.generateKeyPool(keyGenerator, salt, KEY_POOL_SIZE);

			// decode boot block using all available ciphers
			for (EncryptionFunction encryption : EncryptionFunction.values())
			{
				for (CipherModeFunction cipherMode : CipherModeFunction.values())
				{
					byte[] payloadCopy = payload.clone();

					// decrypt payload using the user key
					CipherImplementation cipher = new CipherImplementation(cipherMode, encryption, userKeyPool, 0, PAYLOAD_SIZE);
					cipher.decrypt(aBlockIndex, payloadCopy, 0, PAYLOAD_SIZE, new int[4]);

					// read header
					int expectedChecksum = getInt32(payloadCopy, 0);

					// verify checksum of boot block
					if (expectedChecksum == computeChecksum(salt, payloadCopy))
					{
						Log.dec();

						// when a boot block is created it's also verified
						if (aVerifyFunctions && (aAccessCredentials.getKeyGeneratorFunction() != keyGenerator || aAccessCredentials.getEncryptionFunction() != encryption))
						{
							System.out.println("hash collision in boot block");

							// a checksum collision has occured!
							return null;
						}

						// create the cipher used to encrypt data blocks
						return new CipherImplementation(cipherMode, encryption, payloadCopy, HEADER_SIZE, aBlockData.length);
					}
				}
			}
		}

		Log.w("incorrect password or not a secure BlockDevice");
		Log.dec();

		return null;
	}


	private static int computeChecksum(byte[] aSalt, byte[] aPayloadCopy)
	{
		return MurmurHash3.hash32(aSalt, CHECKSUM_SEED) ^ MurmurHash3.hash32(aPayloadCopy, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);
	}


	@Override
	public void writeBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final int[] aIV)
	{
		assert aBlockIndex >= 0;
		assert aIV.length == 4;

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipherImplementation.encrypt(mBootBlockCount + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aIV);

		mBlockDevice.writeBlock(mBootBlockCount + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, (int[])null); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final int[] aIV)
	{
		assert aBlockIndex >= 0;
		assert aIV.length == 4;

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(mBootBlockCount + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, (int[])null); // block key is used by this blockdevice and not passed to lower levels

		mCipherImplementation.decrypt(mBootBlockCount + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV);

		Log.dec();
	}


	@Override
	public int getBlockSize()
	{
		return mBlockDevice.getBlockSize();
	}


	@Override
	public long size()
	{
		return mBlockDevice.size() - mBootBlockCount;
	}


	@Override
	public void commit(int aIndex, boolean aMetadata)
	{
		mBlockDevice.commit(aIndex, aMetadata);
	}


	@Override
	public void resize(long aNumberOfBlocks)
	{
		mBlockDevice.resize(aNumberOfBlocks + mBootBlockCount);
	}


	@Override
	public void close()
	{
		if (mCipherImplementation != null)
		{
			mCipherImplementation.reset();
			mCipherImplementation = null;
		}

		if (mBlockDevice != null)
		{
			mBlockDevice.close();
			mBlockDevice = null;
		}
	}


	private static final class CipherImplementation
	{
		private transient final int[][] mMasterIV;
		private transient final BlockCipher[] mCiphers;
		private transient final CipherMode mCipherMode;
		private transient final int mUnitSize;
		private transient BlockCipher mTweakCipher;


		public CipherImplementation(final CipherModeFunction aCipherModeFunction, final EncryptionFunction aEncryptionFunction, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitSize)
		{
			mUnitSize = aUnitSize;
			mCipherMode = aCipherModeFunction.newInstance();
			mCiphers = aEncryptionFunction.newInstance();
			mTweakCipher = aEncryptionFunction.newTweakInstance();
			mMasterIV = new int[mCiphers.length][4];

			int offset = aKeyPoolOffset;

			mTweakCipher.engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
			offset += KEY_SIZE_BYTES;

			for (int i = 0; i < 3; i++)
			{
				if (i < mCiphers.length)
				{
					mCiphers[i].engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
				}
				offset += KEY_SIZE_BYTES;
			}

			for (int i = 0; i < 3; i++)
			{
				if (i < mMasterIV.length)
				{
					mMasterIV[i][0] = getInt32(aKeyPool, offset + 0);
					mMasterIV[i][1] = getInt32(aKeyPool, offset + 4);
					mMasterIV[i][2] = getInt32(aKeyPool, offset + 8);
					mMasterIV[i][3] = getInt32(aKeyPool, offset + 12);
				}
				offset += IV_SIZE;
			}
		}


		public void encrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final int[] aBlockIV)
		{
			int[] tmp = new int[4];
			for (int i = 0; i < mCiphers.length; i++)
			{
				tmp[0] = mMasterIV[i][0] ^ aBlockIV[0];
				tmp[1] = mMasterIV[i][1] ^ aBlockIV[1];
				tmp[2] = mMasterIV[i][2] ^ aBlockIV[2];
				tmp[3] = mMasterIV[i][3] ^ aBlockIV[3];
				mCipherMode.encrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, Math.min(mUnitSize, aLength), tmp, mTweakCipher);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final int[] aBlockIV)
		{
			int[] tmp = new int[4];
			for (int i = mCiphers.length; --i >= 0;)
			{
				tmp[0] = mMasterIV[i][0] ^ aBlockIV[0];
				tmp[1] = mMasterIV[i][1] ^ aBlockIV[1];
				tmp[2] = mMasterIV[i][2] ^ aBlockIV[2];
				tmp[3] = mMasterIV[i][3] ^ aBlockIV[3];
				mCipherMode.decrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, Math.min(mUnitSize, aLength), tmp, mTweakCipher);
			}
		}


		private void reset()
		{
			for (BlockCipher cipher : mCiphers)
			{
				if (cipher != null)
				{
					cipher.engineReset();
				}
			}

			for (int[] masterIV : mMasterIV)
			{
				for (int j = 0; j < masterIV.length; j++)
				{
					masterIV[j] = 0;
				}
			}

			fill(mCiphers, null);
		}
	};
}
