package org.terifan.raccoon.blockdevice.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.raccoon.blockdevice.RaccoonIOException;


public class PathUtils
{
	/**
	 * Produces a Path to a file under the user home path.
	 *
	 * @param aRelativePath relative path under under user home directory
	 */
	public static Path produceUserHomePath(String aRelativePath)
	{
		File dir = new File(System.getProperty("user.home"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("User diretory not found in this environment.");
		}
		Path path = new File(dir, aRelativePath).toPath();
		try
		{
			Files.createDirectories(path.getParent());
		}
		catch (IOException e)
		{
			throw new RaccoonIOException("Failed to create path: " + path);
		}
		return path;
	}


	/**
	 * Produces a Path to a file under the system temporary directory with the prefix provided.
	 *
	 * @param aPrefix file name prefix
	 */
	public static Path produceTemporaryPath(String aPrefix)
	{
		try
		{
			Path path = Files.createTempFile(aPrefix, ".rdb");
			path.toFile().deleteOnExit();
			return path;
		}
		catch (IOException e)
		{
			throw new IllegalStateException("Failed to create file in temporary directory", e);
		}
	}


	/**
	 * Produces a Path to a file under the AppData directory at the name and path.
	 *
	 * @param aAppName the name of the app
	 * @param aRelativePath relative path under the appdata directory
	 */
	public static Path produceAppDataPath(String aAppName, String aRelativePath)
	{
		try
		{
			String root = System.getenv("APPDATA");

			if (root != null && !root.isEmpty())
			{
				Path path = Paths.get(root, aAppName, aRelativePath);
				Files.createDirectories(path.getParent());
				return path;
			}

			root = System.getProperty("user.home");

			if (root != null && !root.isEmpty())
			{
				Path path = Paths.get(root, "AppData", "Roaming", aAppName, aRelativePath);
				Files.createDirectories(path.getParent());
				return path;
			}

			return null;
		}
		catch (Exception | Error e)
		{
			throw new IllegalStateException(e);
		}
	}
}
