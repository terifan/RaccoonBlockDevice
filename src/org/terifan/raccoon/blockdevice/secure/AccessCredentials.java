package org.terifan.raccoon.blockdevice.secure;

import org.terifan.raccoon.security.messagedigest.SCrypt;
import org.terifan.raccoon.security.messagedigest.HMAC;


public final class AccessCredentials implements Cloneable
{
	public final static EncryptionFunction DEFAULT_ENCRYPTION = EncryptionFunction.AES;
	public final static CipherModeFunction DEFAULT_CIPHER_MODE = CipherModeFunction.XTS;
	public final static KeyGenerationFunction DEFAULT_KEY_GENERATOR = KeyGenerationFunction.SHA512;

	public final static int DEFAULT_COST = 1024;
	public final static int DEFAULT_ROUNDS = 32;
	public final static int DEFAULT_PARALLELIZATION = 1;
	public final static int DEFAULT_ITERATIONS = 1000;

	private transient EncryptionFunction mEncryptionFunction;
	private transient KeyGenerationFunction mKeyGeneratorFunction;
	private transient CipherModeFunction mCipherModeFunction;
	private transient int mParallelization;
	private transient int mIterations;
	private transient int mRounds;
	private transient int mCost;
	private transient byte[] mPassword;


	public AccessCredentials(Object aPassword)
	{
		mEncryptionFunction = DEFAULT_ENCRYPTION;
		mKeyGeneratorFunction = DEFAULT_KEY_GENERATOR;
		mCipherModeFunction = DEFAULT_CIPHER_MODE;
		mCost = DEFAULT_COST;
		mRounds = DEFAULT_ROUNDS;
		mParallelization = DEFAULT_PARALLELIZATION;
		mIterations = DEFAULT_ITERATIONS;

		if (aPassword != null)
		{
			setPassword(aPassword);
		}
	}


	public AccessCredentials setPassword(Object aPassword)
	{
		if (aPassword instanceof String v)
		{
			aPassword = v.toCharArray();
		}
		if (aPassword instanceof char[] v)
		{
			mPassword = new byte[2 * v.length];
			for (int i = 0, j = 0; i < v.length; i++)
			{
				char c = v[i];
				mPassword[j++] = (byte)(c >>> 8);
				mPassword[j++] = (byte)(c);
			}
		}
		else if (aPassword instanceof byte[] v)
		{
			mPassword = v.clone();
		}
		else
		{
			throw new IllegalArgumentException("Unsupported format; must by byte array, char array or a String");
		}
		return this;
	}


	public AccessCredentials setEncryptionFunction(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public AccessCredentials setKeyGeneratorFunction(KeyGenerationFunction aKeyGeneratorFunction)
	{
		mKeyGeneratorFunction = aKeyGeneratorFunction;
		return this;
	}


	public AccessCredentials setCipherModeFunction(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public AccessCredentials setCost(int aCost)
	{
		mCost = aCost;
		return this;
	}


	public AccessCredentials setParallelization(int aParallelization)
	{
		mParallelization = aParallelization;
		return this;
	}


	public AccessCredentials setRounds(int aRounds)
	{
		mRounds = aRounds;
		return this;
	}


	public AccessCredentials setIterationCount(int aIterationCount)
	{
		mIterations = aIterationCount;
		return this;
	}


	EncryptionFunction getEncryptionFunction()
	{
		return mEncryptionFunction;
	}


	CipherModeFunction getCipherModeFunction()
	{
		return mCipherModeFunction;
	}


	KeyGenerationFunction getKeyGeneratorFunction()
	{
		return mKeyGeneratorFunction;
	}


	byte[] generateKeyPool(byte[] aSalt, int aPoolSize)
	{
		if (mRounds < 1)
		{
			throw new IllegalArgumentException("mRounds < 1");
		}
		if (mParallelization < 1)
		{
			throw new IllegalArgumentException("mParallelization < 1");
		}
		if (mIterations < 1)
		{
			throw new IllegalArgumentException("mIterationCount < 1");
		}
		if (mCost < 0)
		{
			throw new IllegalArgumentException("mCost < 0");
		}

		HMAC mac = new HMAC(mKeyGeneratorFunction.newInstance(), mPassword);

		byte[] pool = SCrypt.generate(mac, aSalt, mCost, mRounds, mParallelization, mIterations, aPoolSize);

		return pool;
	}


	@Override
	protected AccessCredentials clone()
	{
		try
		{
			AccessCredentials instance = (AccessCredentials)super.clone();
			instance.setPassword(mPassword.clone());
			return instance;
		}
		catch (CloneNotSupportedException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
