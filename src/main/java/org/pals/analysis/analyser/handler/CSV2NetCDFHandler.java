package org.pals.analysis.analyser.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.UUID;

import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.pals.analysis.analyser.handler.dao.CSV2NetCDFDao;
import org.pals.analysis.analyser.handler.dao.PalsREngine;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * A Handler knows what needs to be done with the analysis parameters and sets
 * up an environment before it hands over the analysis to an analysisExecutor.
 * 
 * CSV2NetCDFHandler retrieves the CSV remote file and passes it and the other
 * parameters to the CSV2NetCDF analysisExecutor. It also passes back the
 * results as a reply. Note: R script used in the analysisExecutor
 * implementation can read/delete files using URLs but this layer handles
 * copying/deleting files. This is to leave the analysisExecutor free from such
 * remote client/server related operations. Such matters are not essential to R
 * analysis script.
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

	public static final String FUNCTION_NAME = "convertSpreadsheetToNcdf";
	public static final String OBS_CSV = "obsCSV";
	public static final String OBS_FLUX = "obsFlux";
	public static final String OBS_MET = "obsMet";
	public static final String USER_NAME = "userName";
	public static final String DATA_SET_NAME = "dataSetName";
	public static final String DATA_SET_VERSION_NAME = "dataSetVersionName";
	public static final String LONGITUDE = "longitude";
	public static final String LATITUDE = "latitude";
	public static final String ELEVATION = "elevation";
	public static final String TOWER_HEIGHT = "towerHeight";
	public static final String FILE_PROTOCOL = "file";
	public static final String HOST = null;
	public static final String CVS_FILE_SUFFIX = ".csv";
	public static final String FLUX_FILE_SUFFIX = ".flux.nc";
	public static final String MET_FILE_SUFFIX = ".met.nc";

	private Map<String, Object> analysisArguments;

	private PalsREngine palsREngine;
	private String inputDataDirPath;
	private String outputDataDirPath;

	// This has to be set before this instance can be used
	// TODO: This should be set by IoC
	private CSV2NetCDFDao csv2NetCDFDao;
	
	public CSV2NetCDFHandler(PalsREngine palsREngine, String inputDataDirPath, String outputDataDirPath)
	{
		this.palsREngine = palsREngine;
		this.inputDataDirPath = inputDataDirPath;
		this.outputDataDirPath = outputDataDirPath;
	}

	public AnalysisReply handleRequest(AnalysisRequest request)
			throws AnalysisException
	{
		LOGGER.debug("handleRequest");
		
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
			 * Currently, it handles only 'file://' URLs
			 */
			if (FILE_PROTOCOL.equals(protocol))
			{
				localCSVFileURL = getFileViaFileProtocol(requestIdStr,
						this.inputDataDirPath, remoteFileURL);
				
				outputFileURLs = convertCSV2NetCDF(localCSVFileURL,
						this.outputDataDirPath, requestIdStr, userName,
						dataSetName, dataSetVersionName, longitude, latitude,
						elevation, towerHeight);
				
				// clean up when all succeeded
				if (!deleteFileViaFileProtocol(localCSVFileURL)) throw new AnalysisException("filed to delete: " + localCSVFileURL.toExternalForm());
				if (!deleteFileViaFileProtocol(remoteFileURL)) throw new AnalysisException("filed to delete: " + localCSVFileURL.toExternalForm());
			}
			else
				throw new AnalysisException("unknown protocol: " + protocol);
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

	private boolean deleteFileViaFileProtocol(URL fileUrl)
	{
		String filePath = fileUrl.getPath();
		File file = new File(filePath);
		boolean isDelete = file.delete();
		return isDelete;
	}

	/**
	 * The role of this method is to run the analysisEngine. The analysisEngine
	 * runs the analysis implementation, which will read the localCSVFileURL and
	 * try to output outcome to specified URLs. It expects the engine to throw
	 * an exception if it fails to produce output files.
	 * 
	 * @param towerHeight
	 * @param elevation2
	 * @param latitude2
	 * @param longitude2
	 * @param dataSetVersionName
	 * @param dataSetName
	 * @param userName
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
	 * @throws AnalysisException
	 * @throws ScriptException
	 */
	private Map<String, Object> convertCSV2NetCDF(URL localCSVFileURL,
			String outputDir, String requestIdStr, String userName,
			String dataSetName, String dataSetVersionName, String longitude,
			String latitude, String elevation, String towerHeight)
			throws AnalysisException
	{
		LOGGER.debug("convertCSV2NetCDF()");
		
		URL fluxNetCDFURL = null;
		URL metNetCDFURL = null;
		try
		{
			fluxNetCDFURL = new URL(FILE_PROTOCOL, HOST, outputDataDirPath
					+ File.separator + requestIdStr + FLUX_FILE_SUFFIX);
			metNetCDFURL = new URL(FILE_PROTOCOL, HOST, outputDataDirPath
					+ File.separator + requestIdStr + MET_FILE_SUFFIX);
		}
		catch (MalformedURLException e)
		{
			throw new AnalysisException(e);
		}

		Map<String, Object> outputNetCDFFiles = null;
		/*
		 * The engine should be set in IoC so it does not have to know its
		 * implementation or be created here with R_LIBS_USER
		 */
		if (this.csv2NetCDFDao == null) try
		{
			this.csv2NetCDFDao = new CSV2NetCDFDao(palsREngine);
		}
		catch (ScriptException e)
		{
			throw new AnalysisException(e);
		}

		outputNetCDFFiles = this.csv2NetCDFDao
				.convertCSV2NetCDF(localCSVFileURL, fluxNetCDFURL,
						metNetCDFURL, userName, dataSetName,
						dataSetVersionName, longitude, latitude, elevation,
						towerHeight);
		return outputNetCDFFiles;
	}

	/**
	 * This copies the client's file to a server file and deletes the file on
	 * the client. NOTE: It ignores the "host" part of the URL, since at present
	 * it can't access another host other than the localhost.
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
				+ CVS_FILE_SUFFIX;
		File newFile = new File(newFilePathStr);

		String remoteFilePath = remoteFileURL.getPath();
		File remoteFile = new File(remoteFilePath);

		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try
		{
			fileInputStream = new FileInputStream(remoteFile);
			inputChannel = fileInputStream.getChannel();
			fileOutputStream = new FileOutputStream(newFile);
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

		URL newFileURL = new URL(FILE_PROTOCOL, HOST, newFile.getPath());

		return newFileURL;
	}
}
