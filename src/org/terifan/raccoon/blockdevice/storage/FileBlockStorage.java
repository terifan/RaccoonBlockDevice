package org.terifan.raccoon.blockdevice.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.terifan.logging.Logger;
import org.terifan.raccoon.blockdevice.RaccoonDeviceException;
import org.terifan.raccoon.blockdevice.managed.SyncMode;


public class FileBlockStorage implements BlockStorage
{
	private final Logger log = Logger.getLogger();

	protected Path mPath;
	protected FileChannel mFileChannel;
	protected FileLock mFileLock;
	protected SyncMode mSyncMode;
	protected int mBlockSize;
	private final boolean mReadOnly;


	public FileBlockStorage(Path aPath)
	{
		this(aPath, 4096, false);
	}


	public FileBlockStorage(Path aPath, int aBlockSize)
	{
		this(aPath, aBlockSize, false);
	}


	public FileBlockStorage(Path aPath, int aBlockSize, boolean aReadOnly)
	{
		mPath = aPath;
		mBlockSize = aBlockSize;
		mSyncMode = SyncMode.DOUBLE;
		mReadOnly = aReadOnly;

		try
		{
			if (mReadOnly)
			{
				mFileChannel = FileChannel.open(mPath, StandardOpenOption.CREATE, StandardOpenOption.READ);
			}
			else
			{
				try
				{
					mFileChannel = FileChannel.open(mPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
				}
				catch (AccessDeniedException e)
				{
					throw new FileAlreadyOpenException("Failed to open file: " + mPath, e);
				}

				try
				{
					mFileLock = mFileChannel.tryLock();
				}
				catch (IOException | OverlappingFileLockException e)
				{
					throw new FileAlreadyOpenException("Failed to lock file: " + mPath, e);
				}
			}
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	public boolean isReadOnly()
	{
		return mReadOnly;
	}


	public void readBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		log.t("read block {} +{}", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFileChannel.read(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	public void writeBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		log.t("write block {} +{}", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFileChannel.write(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		log.t("read block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFileChannel.read(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		log.t("write block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFileChannel.write(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	@Override
	public void close()
	{
		log.d("close");

		synchronized (this)
		{
			if (mSyncMode == SyncMode.ONCLOSE)
			{
				try
				{
					mFileChannel.force(true);
				}
				catch (IOException e)
				{
					throw new RaccoonDeviceException(e);
				}
			}

			if (mFileLock != null)
			{
				try
				{
					mFileLock.release();
					mFileLock.close();
					mFileLock = null;
				}
				catch (Throwable e)
				{
					log.e("Unhandled error when releasing file lock", e);
				}
			}

			if (mFileChannel != null)
			{
				try
				{
					mFileChannel.close();
				}
				catch (IOException e)
				{
					throw new RaccoonDeviceException(e);
				}
				mFileChannel = null;
			}
		}
	}


	@Override
	public long size()
	{
		if (mFileChannel == null)
		{
			try
			{
				return Files.size(mPath) / mBlockSize;
			}
			catch (IOException e)
			{
				throw new IllegalStateException(e);
			}
		}
		try
		{
			return mFileChannel.size() / mBlockSize;
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	@Override
	public void commit(int aIndex, boolean aMetadata)
	{
		log.d("commit");

		if (aIndex == 0 && mSyncMode != SyncMode.OFF || aIndex == 1 && mSyncMode == SyncMode.DOUBLE)
		{
			try
			{
				mFileChannel.force(aMetadata);
			}
			catch (IOException e)
			{
				throw new RaccoonDeviceException(e);
			}
		}
	}


	@Override
	public int getBlockSize()
	{
		return mBlockSize;
	}


	@Override
	public void resize(long aNumberOfBlocks)
	{
		try
		{
			mFileChannel.truncate(aNumberOfBlocks * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonDeviceException(e);
		}
	}


	public FileBlockStorage setSyncMode(SyncMode aSyncMode)
	{
		mSyncMode = aSyncMode;
		return this;
	}


	public SyncMode getSyncMode()
	{
		return mSyncMode;
	}
}
