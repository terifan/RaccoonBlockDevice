package org.terifan.raccoon.blockdevice.managed;

import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;


public class RangeMap implements Cloneable
{
	private TreeMap<Long, Long> mMap;
	private long mSpace;


	public RangeMap()
	{
		mMap = new TreeMap<>();
		mSpace = 0;
	}


	public synchronized void add(long aOffset, long aSize)
	{
		if (aOffset < 0 || aSize <= 0)
		{
			throw new IllegalArgumentException("Illegal range: offset: " + aOffset + ", size: " + aSize);
		}

		long start = aOffset;
		long end = aOffset + aSize;

		assert end > start : end + " > " + start;

		Long before = mMap.floorKey(start);
		Long after = mMap.ceilingKey(start);

		Long v1 = mMap.lowerKey(end);
		Long v2 = mMap.lowerKey(start);

		if (v1 != null && v1 >= start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (1): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}
		if (v2 != null && v2 < start && mMap.get(v2) > start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (2): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}

		boolean mergeBefore = (before != null && mMap.get(before) == start);
		boolean mergeAfter = (after != null && after == end);

		if (mergeBefore && mergeAfter)
		{
			mMap.put(before, mMap.remove(after));
		}
		else if (mergeBefore)
		{
			mMap.put(before, end);
		}
		else if (mergeAfter)
		{
			mMap.put(start, mMap.remove(after));
		}
		else
		{
			mMap.put(start, end);
		}

		mSpace += aSize;
	}


	public synchronized void remove(long aOffset, long aSize)
	{
		if (aSize <= 0)
		{
			throw new IllegalArgumentException("Size is zero or negative: size: " + aSize);
		}
		if (aOffset < 0)
		{
			throw new IllegalArgumentException("Offset is negative: offset: " + aOffset);
		}

		long start = aOffset;
		long end = aOffset + aSize;

		Long blockStart = mMap.floorKey(start);

		if (blockStart == null)
		{
			throw new IllegalArgumentException("No free block at offset: offset: " + start);
		}

		long blockEnd = mMap.get(blockStart);

		if (end > blockEnd)
		{
			throw new IllegalArgumentException("Block size shorter than requested size: remove: " + start + "-" + end + ", from block: " + blockStart + "-" + blockEnd);
		}

		boolean leftOver = start != blockStart;
		boolean rightOver = end != blockEnd;

		if (leftOver && rightOver)
		{
			mMap.put(blockStart, start);
			mMap.put(end, blockEnd);
		}
		else if (leftOver)
		{
			mMap.put(blockStart, start);
		}
		else if (rightOver)
		{
			mMap.remove(blockStart);
			mMap.put(end, blockEnd);
		}
		else
		{
			mMap.remove(blockStart);
		}

		mSpace -= aSize;
	}


	public synchronized long next(long aSize)
	{
		Entry<Long, Long> entry = mMap.firstEntry();

		for (;;)
		{
			if (entry == null)
			{
				return -1;
			}

			long offset = entry.getKey();

			if (entry.getValue() - offset >= aSize)
			{
				remove(offset, aSize);

				return offset;
			}

			entry = mMap.higherEntry(offset);
		}
	}


	public synchronized long getFreeSpace()
	{
		return mSpace;
	}


	public synchronized long getUsedSpace()
	{
		return mMap.lastEntry().getValue() - mSpace;
	}


	public synchronized boolean isFree(long aOffset, long aSize)
	{
		Long blockStart = mMap.floorKey(aOffset);

		if (blockStart != null)
		{
			long blockEnd = mMap.get(blockStart) - 1;

			if (blockEnd >= aOffset + aSize || blockEnd >= aOffset)
			{
				return false;
			}
		}

		return true;
	}


	public synchronized void clear()
	{
		mMap.clear();
		mSpace = 0;
	}


	@Override
	public RangeMap clone()
	{
		try
		{
			RangeMap map = (RangeMap)super.clone();
			map.mMap = new TreeMap<>(mMap);
			return map;
		}
		catch (CloneNotSupportedException e)
		{
			throw new IllegalStateException(e);
		}
	}


	public void marshal(ByteArrayBuffer aDataOutput)
	{
		long prev = 0;

		aDataOutput.writeVar64U(mMap.size());

		for (Entry<Long, Long> entry : mMap.entrySet())
		{
			long index = entry.getKey();

			aDataOutput.writeVar64U(entry.getValue() - index);
			aDataOutput.writeVar64U(index - prev);

			prev = index;
		}
	}


	public void unmarshal(ByteArrayBuffer aDataInput)
	{
		long size = aDataInput.readVar64U();

		for (long i = 0, prev = 0; i < size; i++)
		{
			long count = aDataInput.readVar64U();

			prev += aDataInput.readVar64U();

			add(prev, count);
		}
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("{");
		for (Entry<Long, Long> entry : mMap.entrySet())
		{
			if (sb.length() > 1)
			{
				sb.append(", ");
			}
			sb.append(entry.getKey() + "-" + (entry.getValue() - 1));
		}
		sb.append("}");
		return sb.toString();
	}


	long getLastBlockIndex()
	{
		return mMap.lastKey();
	}
}
