package org.terifan.raccoon.blockdevice.storage;

import org.terifan.raccoon.blockdevice.BlockDevice;
import org.terifan.raccoon.blockdevice.DeviceAccessOptions;


public interface BlockStorage extends BlockDevice
{
	public void open(DeviceAccessOptions aOptions);
}
