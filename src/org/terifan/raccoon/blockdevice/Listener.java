package org.terifan.raccoon.blockdevice;

import java.io.IOException;


@FunctionalInterface
public interface Listener<T>
{
	void call(T aValue) throws IOException;
}
