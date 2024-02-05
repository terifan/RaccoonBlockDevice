package org.terifan.raccoon.blockdevice.util;

import java.util.Locale;


public class ValueFormatter
{
	/**
	 * Format byte size into seven characters or less.
	 */
	public static String formatBytesSize(long aBytes)
	{
		if (aBytes < 1024)
		{
			return aBytes + " B";
		}
		if (aBytes < 1024 * 1024 * 10)
		{
			return aBytes / 1024 + " KB";
		}
		if (aBytes < 1024 * 1024 * 1024L * 10)
		{
			return aBytes / 1024 / 1024 + " MB";
		}
		if (aBytes < 1024 * 1024 * 1024L * 1024L * 10)
		{
			return aBytes / 1024 / 1024 / 1024 + " GB";
		}
		return aBytes / 1024 / 1024 / 1024 / 1024 + " TB";
	}


	/**
	 * Format byte size into seven characters or less.
	 */
	public static String formatBytesPerSecond(long aBytesPerSecond)
	{
		if (aBytesPerSecond < 1024)
		{
			return aBytesPerSecond + " B/s";
		}
		if (aBytesPerSecond < 1024 * 1024)
		{
			return String.format(Locale.US, "%.1f KB/s", aBytesPerSecond / 1024.0);
		}
		return String.format(Locale.US, "%.1f MB/s", aBytesPerSecond / 1024.0 / 1024.0);
	}


	public static String formatDuration(long aMillis)
	{
		if (aMillis < 0)
		{
			aMillis = System.currentTimeMillis() + aMillis;
		}
		if (aMillis < 1000)
		{
			return String.format("0.%03d", aMillis);
		}
		if (aMillis < 60 * 1000)
		{
			return String.format("%d.%03d", aMillis / 1000, aMillis % 1000);
		}
		if (aMillis < 60 * 60 * 1000)
		{
			return String.format("%2d:%02d.%03d", aMillis / 60000, (aMillis % 60000) / 1000, aMillis % 1000);
		}
		return String.format("%d:%02d:%02d", aMillis / 360000, (aMillis / 60000) % 60, (aMillis % 60000) / 1000);
	}


	public static String formatSeconds(long aMillis)
	{
		if (aMillis < 1000)
		{
			return String.format(Locale.US, "0.%03ds", aMillis);
		}
		return String.format(Locale.US, "%,d.%03ds", aMillis / 1000, aMillis % 1000);
	}


	public static String formatCount(long aCount)
	{
		return String.format(Locale.US, "%,d", aCount);
	}


	public static void main(String ... args)
	{
		try
		{
			System.out.printf("%7s%n", formatBytesSize(100));
			System.out.printf("%7s%n", formatBytesSize(1000));
			System.out.printf("%7s%n", formatBytesSize(10000));
			System.out.printf("%7s%n", formatBytesSize(1000000));
			System.out.printf("%7s%n", formatBytesSize(10000000));
			System.out.printf("%7s%n", formatBytesSize(100000000));
			System.out.printf("%7s%n", formatBytesSize(1000000000));
			System.out.printf("%7s%n", formatBytesSize(10000000000L));
			System.out.printf("%7s%n", formatBytesSize(100000000000L));
			System.out.printf("%7s%n", formatBytesSize(1000000000000L));
			System.out.printf("%7s%n", formatBytesSize(10000000000000L));
			System.out.printf("%7s%n", formatBytesSize(100000000000000L));

			System.out.printf("%10s%n", formatBytesPerSecond(100));
			System.out.printf("%10s%n", formatBytesPerSecond(1000));
			System.out.printf("%10s%n", formatBytesPerSecond(10000));
			System.out.printf("%10s%n", formatBytesPerSecond(100000));
			System.out.printf("%10s%n", formatBytesPerSecond(1000000));
			System.out.printf("%10s%n", formatBytesPerSecond(10000000));
			System.out.printf("%10s%n", formatBytesPerSecond(100000000));
			System.out.printf("%10s%n", formatBytesPerSecond(1000000000));

			System.out.printf("%9s%n", formatDuration(100));
			System.out.printf("%9s%n", formatDuration(1000));
			System.out.printf("%9s%n", formatDuration(10000));
			System.out.printf("%9s%n", formatDuration(100000));
			System.out.printf("%9s%n", formatDuration(1000000));
			System.out.printf("%9s%n", formatDuration(10000000));
			System.out.printf("%9s%n", formatDuration(100000000));

			System.out.printf("%12s%n", formatSeconds(100));
			System.out.printf("%12s%n", formatSeconds(1000));
			System.out.printf("%12s%n", formatSeconds(10000));
			System.out.printf("%12s%n", formatSeconds(100000));
			System.out.printf("%12s%n", formatSeconds(1000000));
			System.out.printf("%12s%n", formatSeconds(10000000));
			System.out.printf("%12s%n", formatSeconds(100000000));

			System.out.printf("%11s%n", formatCount(100));
			System.out.printf("%11s%n", formatCount(1000));
			System.out.printf("%11s%n", formatCount(10000));
			System.out.printf("%11s%n", formatCount(100000));
			System.out.printf("%11s%n", formatCount(1000000));
			System.out.printf("%11s%n", formatCount(10000000));
			System.out.printf("%11s%n", formatCount(100000000));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
