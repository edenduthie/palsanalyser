package org.pals.analysis.analyser.handler.remoteFileHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * File protocol handler is a trivial class which simply uses File operations
 * with URL. It discards the "protocol://user@host" part of the information.
 * 
 * @author Yoichi
 * 
 */
public class FileProtocolHandler implements RemoteFileHandler
{
	public void copyRemoteFileToLocal(URL remoteFileUrl, File localFile)
			throws IOException
	{
		String remoteFilePath = remoteFileUrl.getPath();
		File remoteFile = new File(remoteFilePath);

		copyFile2File(remoteFile, localFile);
	}

	/**
	 * This copies the temp files to designated "store" location, where the
	 * client expects the files to be. This currently ignores the user and host
	 * as in file://user@host/xyz. It also just do normal local file operation
	 * with Java File. So, it does not really have to use the URL form. The
	 * reason why the URL form is used is the compatibility. The API needs to
	 * use URL for cases when files are stored using other URL protocols.
	 * 
	 * @throws IOException
	 */
	public void storeFilesIntoStore(Map<String, File> localFiles,
			Map<String, URL> remoteUrls) throws IOException
	{
		URL remoteFileUrl;
		String remoteFilePath;
		File remoteFile;
		File localFile;
		for (String key : localFiles.keySet())
		{
			localFile = localFiles.get(key);
			remoteFileUrl = remoteUrls.get(key);
			remoteFilePath = remoteFileUrl.getPath();
			remoteFile = new File(remoteFilePath);
			copyFile2File(localFile, remoteFile);
		}
	}
	
	/**
	 * Actual copy method
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	private void copyFile2File(File fromFile, File toFile) throws IOException
	{
		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try
		{
			fileInputStream = new FileInputStream(fromFile);
			inputChannel = fileInputStream.getChannel();
			fileOutputStream = new FileOutputStream(toFile);
			outputChannel = fileOutputStream.getChannel();

			long position = 0;
			outputChannel.transferFrom(inputChannel, position,
					inputChannel.size());
		}
		finally
		{
			if (inputChannel != null) inputChannel.close();
			if (fileInputStream != null) fileInputStream.close();
			if (outputChannel != null) outputChannel.close();
			if (fileOutputStream != null) fileOutputStream.close();
		}
	}

	/**
	 * This returns the path without the file name, i.e. remote data directory
	 * URL
	 */
	public URL getStoreUrl(URL remoteFileUrl)
	{
		String urlStr = remoteFileUrl.toString();
		String fileNameStr = remoteFileUrl.getFile();
		String remoteDirUrlStr = urlStr.replace(fileNameStr, "");
		URL remoteDirUrl = null;
		try
		{
			remoteDirUrl = new URL(remoteDirUrlStr);
		}
		catch (MalformedURLException e)
		{
			// ignore. This should not happen
		}
		return remoteDirUrl;
	}
}
