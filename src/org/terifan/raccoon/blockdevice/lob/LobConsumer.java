package org.terifan.raccoon.blockdevice.lob;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public interface LobConsumer
{
    void accept(ObjectId aObjectId, Document aMetadata);
}
