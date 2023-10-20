package org.terifan.raccoon.blockdevice;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public interface LobConsumer
{
    void accept(ObjectId aObjectId, Document aMetadata);
}
