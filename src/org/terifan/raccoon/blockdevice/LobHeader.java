package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.document.Document;


public class LobHeader
{
	Document mData;


	public LobHeader()
	{
	}


	public LobHeader(Document aData)
	{
		mData = aData;
	}


	public Document marshal()
	{
		return mData;
	}
}
