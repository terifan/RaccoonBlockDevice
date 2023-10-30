package org.terifan.raccoon.blockdevice.util;

import java.io.PrintStream;
import java.nio.charset.Charset;


public class Log
{
	public final static PrintStream out = System.out;

	private static LogLevel mLevel = LogLevel.ERROR;
	private static int mIndent;


	public static void setLevel(LogLevel aLevel)
	{
		mLevel = aLevel;
	}


	public static void inc()
	{
		mIndent++;
	}


	public static void dec()
	{
		mIndent--;
	}


	public static void s(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.FATAL, aMessage, aParams);
	}


	public static void e(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.ERROR, aMessage, aParams);
	}


	public static void w(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.WARN, aMessage, aParams);
	}


	public static void i(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.INFO, aMessage, aParams);
	}


	public static void d(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.DEBUG, aMessage, aParams);
	}


	private static void logImpl(LogLevel aLevel, String aMessage, Object... aParams)
	{
		if (aLevel.ordinal() >= mLevel.ordinal() && aMessage != null)
		{
			StringBuilder message = new StringBuilder();
			for (int i = 0; i < mIndent; i++)
			{
				message.append("... ");
			}
			message.append(Console.format(aMessage, aParams));

			StackTraceElement[] trace = Thread.currentThread().getStackTrace();
			String className = trace[3].getClassName();
			className = className.substring(className.lastIndexOf('.') + 1);
			String methodName = trace[3].getMethodName();
			String loggerName = trace[3].getFileName() + ":" + trace[3].getLineNumber();

			System.out.printf("%-30s  %-30s  %-30s  %-7s  %s%n", shrink(loggerName,30), shrink(className,30), shrink(methodName,30), aLevel, message.toString());
		}
	}


	private static String shrink(String aText, int aLength)
	{
		return aText.length() <= aLength ? aText : aText.substring(0, aLength / 2 - 2) + "[..]" + aText.substring(aText.length() - aLength / 2 + 2);
	}


	public static void hexDump(byte[] aBuffer)
	{
		hexDump(aBuffer, 32, null);
	}


	public static void hexDump(byte[] aBuffer, int LW, byte[] aCompareWith)
	{
		int MR = aBuffer.length;

		StringBuilder binText = new StringBuilder("");
		StringBuilder hexText = new StringBuilder("");

		for (int row = 0, offset = 0; offset < aBuffer.length && row < MR; row++)
		{
			hexText.append("\033[1;30m" + String.format("%04d: ", row * LW) + "\033[0m");

			int padding = 3 * LW + LW / 8;
			String mode = "";

			for (int i = 0; offset < aBuffer.length && i < LW; i++, offset++)
			{
				int c = 0xff & aBuffer[offset];

				String nextMode;
				if (aCompareWith != null && c != (0xff & aCompareWith[offset]))
				{
					nextMode = "\033[1;31m";
				}
				else if (c >= '0' && c <= '9')
				{
					nextMode = "\033[0;35m";
				}
				else if (!(c < ' ' || c >= 128))
				{
					nextMode = "\033[0;36m";
				}
				else
				{
					nextMode = "\033[0m";
				}
				if (!nextMode.equals(mode))
				{
					mode = nextMode;
					hexText.append(mode);
					binText.append(mode);
				}

				hexText.append(String.format("%02x ", c));
				binText.append(Character.isISOControl(c) ? '.' : (char)c);

				padding -= 3;

				if ((i & 7) == 7)
				{
					hexText.append(" ");
					padding--;
				}
			}

			hexText.append(" ".repeat(padding));

			System.out.println(hexText + "\033[0m" + binText + "\033[0m");

			binText.setLength(0);
			hexText.setLength(0);
		}
	}


	public static String toHex(byte[] aValue)
	{
		StringBuilder sb = new StringBuilder();
		for (byte b : aValue)
		{
			sb.append(String.format("%02X", b));
		}
		return sb.toString();
	}


	public static String toString(byte[] aValue)
	{
		return aValue == null ? null : new String(aValue, Charset.defaultCharset());
	}


	public static void diffDump(byte[] aBuffer1, int LW, byte[] aBuffer2)
	{
		int MR = aBuffer1.length;

		StringBuilder binText1 = new StringBuilder("");
		StringBuilder hexText1 = new StringBuilder("");
		StringBuilder binText2 = new StringBuilder("");
		StringBuilder hexText2 = new StringBuilder("");

		int len = Math.max(aBuffer1.length, aBuffer2.length);

		for (int row = 0, xoffset = 0; xoffset < len && row < MR; row++, xoffset+=LW)
		{
			hexText1.append("\033[1;30m" + String.format("%05d: ", row * LW) + "\033[0m");

			int padding = 3 * LW + LW / 8;
			String mode = "";
			String output = "";

			for (int i = 0, offset = xoffset; offset < len && i < LW; i++, offset++)
			{
				int ch = offset >= aBuffer1.length ? 0 : 0xff & aBuffer1[offset];
				int dh = offset >= aBuffer2.length ? 0 : 0xff & aBuffer2[offset];

				if (offset >= aBuffer1.length || offset >= aBuffer2.length)
				{
					if (mode != null)
					{
						mode = null;
						String cl = "\033[0;36m";
						if (offset >= aBuffer1.length)
						{
							hexText1.append(cl);
							binText1.append(cl);
						}
						else
						{
							hexText2.append(cl);
							binText2.append(cl);
						}
					}
				}
				else
				{
					String nextMode = ch != dh ? "\033[1;31m" : "\033[0m";
					if (!nextMode.equals(mode))
					{
						mode = nextMode;
						hexText1.append(mode);
						binText1.append(mode);
						hexText2.append(mode);
						binText2.append(mode);
					}
				}

				hexText1.append(String.format("%02x ", ch));
				binText1.append(ch >= 32 && ch < 128 ? (char)ch : '.');
				hexText2.append(String.format("%02x ", dh));
				binText2.append(dh >= 32 && dh < 128 ? (char)dh : '.');

				padding -= 3;

				if ((i & 7) == 7)
				{
					hexText1.append(" ");
					hexText2.append(" ");
					padding--;
				}
			}

			hexText1.append("\033[0m").append(" ".repeat(padding));
			hexText2.append("\033[0m").append(" ".repeat(padding));

			output += hexText1 + "" + binText1 + "\033[0m" + (" ".repeat(Math.max(0,padding/3))) + "    " + hexText2 + "" + binText2 + "\033[0m";

			binText1.setLength(0);
			hexText1.setLength(0);
			binText2.setLength(0);
			hexText2.setLength(0);

			System.out.println(output);
		}
	}
}
