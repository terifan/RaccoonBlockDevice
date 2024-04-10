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
import org.terifan.raccoon.blockdevice.BlockDeviceOpenOption;
import org.terifan.raccoon.blockdevice.RaccoonIOException;
import org.terifan.raccoon.blockdevice.managed.SyncMode;


public class FileBlockStorage extends BlockStorage<FileBlockStorage>
{
	private final Logger log = Logger.getLogger();
	private final static int DEFAULT_BLOCK_SIZE = 4096;

	protected Path mPath;
	protected FileChannel mFileChannel;
	protected FileLock mFileLock;
	protected SyncMode mSyncMode;
	protected int mBlockSize;
	protected boolean mReadOnly;


	public FileBlockStorage(Path aPath)
	{
		this(aPath, DEFAULT_BLOCK_SIZE);
	}


	public FileBlockStorage(Path aPath, int aBlockSize)
	{
		mPath = aPath;
		mBlockSize = aBlockSize;
		mSyncMode = SyncMode.DOUBLE;
	}


	@Override
	public FileBlockStorage open(BlockDeviceOpenOption aOption)
	{
		setOpenState();

		mReadOnly = aOption == BlockDeviceOpenOption.READ_ONLY;

		try
		{
			if (Files.exists(mPath))
			{
				if (aOption == BlockDeviceOpenOption.REPLACE)
				{
					if (!Files.deleteIfExists(mPath))
					{
						throw new RaccoonIOException("Failed to delete existing file: " + mPath);
					}
				}
				else if (mReadOnly && Files.size(mPath) == 0)
				{
					throw new RaccoonIOException("File is empty.");
				}
			}
			else if (mReadOnly || aOption == BlockDeviceOpenOption.OPEN)
			{
				throw new RaccoonIOException("File not found: " + mPath);
			}

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
			throw new RaccoonIOException(e);
		}

		return this;
	}


	@Override
	public boolean isReadOnly()
	{
		return mReadOnly;
	}


	public void readBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		assertOpen();

		log.t("read block {} +{}", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFileChannel.read(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException(e);
		}
	}


	public void writeBlock(long aBlockIndex, ByteBuffer aBuffer, long[] aBlockKey)
	{
		assertOpen();

		log.t("write block {} +{}", aBlockIndex, (aBuffer.limit() - aBuffer.position()) / mBlockSize);

		try
		{
			mFileChannel.write(aBuffer, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException(e);
		}
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assertOpen();

		log.t("read block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFileChannel.read(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException | IndexOutOfBoundsException e)
		{
			throw new RaccoonIOException("available=" + aBuffer.length + ", offset=" + aBufferOffset + ", length=" + aBufferLength, e);
		}
	}


	@Override
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, int[] aBlockKey)
	{
		assertOpen();

		log.t("write block {} +{}", aBlockIndex, aBufferLength / mBlockSize);

		try
		{
			ByteBuffer buf = ByteBuffer.wrap(aBuffer, aBufferOffset, aBufferLength);
			mFileChannel.write(buf, aBlockIndex * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException(e);
		}
	}


	@Override
	public void close()
	{
		log.d("close");

		synchronized (this)
		{
			try
			{
				if (mSyncMode == SyncMode.ONCLOSE)
				{
					try
					{
						mFileChannel.force(true);
					}
					catch (Exception | Error e)
					{
						throw new RaccoonIOException(e);
					}
				}
				if (mFileLock != null)
				{
					try
					{
						mFileLock.release();
						mFileLock.close();
					}
					catch (Exception | Error e)
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
					catch (Exception | Error e)
					{
						throw new RaccoonIOException(e);
					}
				}
			}
			finally
			{
				mFileLock = null;
				mFileChannel = null;
				setClosedState();
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
			throw new RaccoonIOException(e);
		}
	}


	@Override
	public void commit(int aIndex, boolean aMetadata)
	{
		assertOpen();

		log.d("commit");

		if (aIndex == 0 && mSyncMode != SyncMode.OFF || aIndex == 1 && mSyncMode == SyncMode.DOUBLE)
		{
			try
			{
				mFileChannel.force(aMetadata);
			}
			catch (IOException e)
			{
				throw new RaccoonIOException(e);
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
		assertOpen();

		try
		{
			mFileChannel.truncate(aNumberOfBlocks * mBlockSize);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException(e);
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
