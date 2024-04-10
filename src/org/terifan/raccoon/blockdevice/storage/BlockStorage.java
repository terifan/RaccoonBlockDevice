package org.terifan.raccoon.blockdevice.storage;

import org.terifan.raccoon.blockdevice.BlockDevice;
import org.terifan.raccoon.blockdevice.BlockDeviceOpenOption;


public abstract class BlockStorage<T extends BlockStorage> implements BlockDevice
{
	private transient boolean mOpen;


	public abstract T open(BlockDeviceOpenOption aOptions);


	public boolean isOpen()
	{
		return mOpen;
	}


	protected void setOpenState()
	{
		assertNotOpen();
		mOpen = true;
	}


	protected void setClosedState()
	{
		assertOpen();
		mOpen = false;
	}


	protected void assertNotOpen()
	{
		if (mOpen)
		{
			throw new IllegalStateException("BlockStorage already open.");
		}
	}


	protected void assertOpen()
	{
		if (!mOpen)
		{
			throw new IllegalStateException("BlockStorage not open.");
		}
	}
}
