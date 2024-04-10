package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;


public class RaccoonStorageInstance
{
	private BlockStorage mBlockStorage;


	RaccoonStorageInstance(BlockStorage aBlockStorage)
	{
		mBlockStorage = aBlockStorage;
	}


	public ManagedBlockDevice open(BlockDeviceOpenOption aOpenOption)
	{
		return new ManagedBlockDevice(mBlockStorage);
	}
}
