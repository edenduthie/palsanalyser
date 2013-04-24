package org.pals.analysis.analyser.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * Handler retrieves the remote file and parameters and pass them to the
 * analysis method. It also pass back the results as a reply. Note: R can
 * read/delete files using URLs but this layer handles copying/deleting files.
 * This is to leave R scripts free from such remote client/server needs. Such
 * are not essential to analysis.
 * 
 * It implements only file://localhost/ URL (or without host). If client and
 * server are on different hosts, it may be better to use URLs other than
 * file://. Java could open URLConnection and read bytes from the source, but it
 * may require code to handle security. (URLConnection works with http:// but we
 * are not using it.)
 * 
 * @author Yoichi
 * 
 */
public class CSV2NetCDFHandler implements RequestHandler
{
	private final static Logger LOGGER = Logger
			.getLogger(CSV2NetCDFHandler.class.getName());

	public static final String OBS_CSV = "obsCSV";
	public static final String USER_NAME = "userName";
	public static final String DATA_SET_NAME = "dataSetName";
	public static final String DATA_SET_VERSION_NAME = "dataSetVersionName";
	public static final String LONGITUDE = "longitude";
	public static final String LATITUDE = "latitude";
	public static final String ELEVATION = "elevation";
	public static final String TOWER_HEIGHT = "towerHeight";
	public static final String FILE_PROTOCOL = "file";
	public static final String FLUX_FILE_SUFFIX = "_flux.nc";
	public static final String MET_FILE_SUFFIX = "_met.nc";

	private Map<String, Object> analysisArguments;

	private String inputDataDirPath;
	private String outputDataDirPath;

	public CSV2NetCDFHandler(String inputDataDirPath, String outputDataDirPath)
	{
		this.inputDataDirPath = inputDataDirPath;
		this.outputDataDirPath = outputDataDirPath;
	}

	public AnalysisReply handleRequest(AnalysisRequest request)
			throws AnalysisException
	{
		AnalysisReply reply = null;

		analysisArguments = request.getAnalysisArguments();

		UUID requestId = request.getRequestId();
		String requestIdStr = requestId.toString();
		String obsCsvURLStr = (String) analysisArguments.get(OBS_CSV);
		String userName = (String) analysisArguments.get(USER_NAME);
		String dataSetName = (String) analysisArguments.get(DATA_SET_NAME);
		String dataSetVersionName = (String) analysisArguments
				.get(DATA_SET_VERSION_NAME);
		String longitude = (String) analysisArguments.get(LONGITUDE);
		String latitude = (String) analysisArguments.get(LATITUDE);
		String elevation = (String) analysisArguments.get(ELEVATION);
		String towerHeight = (String) analysisArguments.get(TOWER_HEIGHT);

		URL remoteFileURL;
		URL localCSVFileURL = null;
		Map<String, Object> outputFileURLs = null;
		try
		{
			remoteFileURL = new URL(obsCsvURLStr);
			String protocol = remoteFileURL.getProtocol();
			/**
			 * Currently, it handles only file:// URLs
			 */
			if (FILE_PROTOCOL.equals(protocol))
			{
				localCSVFileURL = getFileViaFileProtocol(requestIdStr,
						this.inputDataDirPath, remoteFileURL);
			}

			else
				throw new AnalysisException("unknown protocol: " + protocol);

			outputFileURLs = convertCSV2NetCDFWithR(localCSVFileURL,
					this.outputDataDirPath, userName, dataSetName,
					dataSetVersionName, longitude, latitude, elevation,
					towerHeight);
		}

		catch (MalformedURLException e)
		{
			throw new AnalysisException(e);
		}
		catch (IOException e)
		{
			throw new AnalysisException(e);
		}

		reply = new AnalysisReply(requestId);
		reply.setStatus(AnalysisReply.Status.NORMAL);
		reply.setAnalysisResults(outputFileURLs);
		return reply;
	}

	/**
	 * Reads the input file from the inputDataDir and makes output files in the
	 * outputDataDir. The input file is deleted after it is processed. The
	 * output files will be deleted by the client.
	 * 
	 * @param localCSVFile
	 * @param userName
	 * @param dataSetName
	 * @param dataSetVersionName
	 * @param longitude2
	 * @param latitude2
	 * @param elevation2
	 * @param towerHeight
	 * @param towerHeight2
	 * @return
	 */
	private Map<String, Object> convertCSV2NetCDFWithR(URL localCSVFileURL,
			String userName, String dataSetName, String dataSetVersionName,
			String longitude2, String latitude2, String elevation2,
			String towerHeight, String towerHeight2)
	{
		// XXX WORK_IN_PROGRESS Auto-generated method stub
		return null;
	}

	/**
	 * This copies the client's file to a server file and deletes the server
	 * file. It ignores the "host" part of the URL.
	 * 
	 * TODO: When both files are on the local fs, the remote file just needs to
	 * be moved/renamed.
	 * 
	 * @param requestId
	 * @param inputDataDirPath
	 * @param remoteFileURL
	 * @return
	 * @throws IOException
	 * @see {@link http
	 *      ://examples.javacodegeeks.com/core-java/io/file/4-ways-to-
	 *      copy-file-in-java/}
	 */
	private URL getFileViaFileProtocol(String requestId,
			String inputDataDirPath, URL remoteFileURL) throws IOException
	{
		File inputFileDir = new File(inputDataDirPath);
		if (!inputFileDir.exists()) inputFileDir.mkdirs();
		String newFilePathStr = inputDataDirPath + File.separator + requestId
				+ ".csv";
		File newFile = new File(newFilePathStr);

		String remoteFilePath = remoteFileURL.getPath();
		File remoteFile = new File(remoteFilePath);

		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		boolean isDeleted = false;
		try
		{
			fileInputStream = new FileInputStream(remoteFile);
			inputChannel = fileInputStream.getChannel();
			fileOutputStream = new FileOutputStream(newFile);
			outputChannel = fileOutputStream.getChannel();

			long position = 0;
			outputChannel.transferFrom(inputChannel, position,
					inputChannel.size());

			isDeleted = remoteFile.delete();
			if (!isDeleted) LOGGER.warning("cound not delete: "
					+ remoteFile.toURI());
		}
		finally
		{
			inputChannel.close();
			if (fileInputStream != null) fileInputStream.close();
			outputChannel.close();
			if (fileOutputStream != null) fileOutputStream.close();
		}

		URL newFileURL = new URL("file", "", newFile.getPath());

		return newFileURL;
	}
}
