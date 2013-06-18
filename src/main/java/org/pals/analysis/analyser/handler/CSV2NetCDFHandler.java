package org.pals.analysis.analyser.handler;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.pals.analysis.analyser.handler.dao.CSV2NetCDFDao;
import org.pals.analysis.analyser.handler.dao.PalsRserveEngine;
import org.pals.analysis.analyser.handler.remoteFileHandler.RemoteFileHandler;
import org.pals.analysis.analyser.handler.remoteFileHandler.UrlProtocolHandlerFactory;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * A Handler knows what needs to be done with the analysis parameters and sets
 * up an environment before it hands over the analysis to an analysisExecutor.
 * 
 * CSV2NetCDFHandler retrieves the CSV remote file and passes its location and
 * other parameters to the CSV2NetCDF analysisExecutor. It gets the results as a
 * reply. Note: R script used in the analysisExecutor implementation can
 * read/delete files using URLs but this layer handles copying/deleting files.
 * This is to leave the analysisExecutor free from such remote client/server
 * related operations. Such matters are not essential to R analysis script,
 * particularly when it is tested on a local machine.
 * 
 * It reads/writes files using URL protocols with regards to remote files, but
 * all local files use File://
 * 
 * TODO: URL protocol handlers should be defined as separate classes. They can
 * share the same API, since this handler knows what the protocol handlers
 * should do but does not care how they do.
 * 
 * Since it is a server, unless it is a bug, it cannot throw an exception to
 * halt.
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
	public static final String CVS_FILE_SUFFIX = ".csv";
	public static final String FLUX_FILE_SUFFIX = ".flux.nc";
	public static final String MET_FILE_SUFFIX = ".met.nc";

	private Map<String, Object> analysisArguments;

	private PalsRserveEngine palsRserveEngine;
	private File inputDataDir;
	private File outputDataDir;

	// This has to be set before this instance can be used
	// TODO: This should be set by IoCs
	private CSV2NetCDFDao csv2NetCDFDao;

	public CSV2NetCDFHandler(PalsRserveEngine palsRserveEngine,
			File inputDataDir, File outputDataDir)
	{
		this.palsRserveEngine = palsRserveEngine;
		this.inputDataDir = inputDataDir;
		this.outputDataDir = outputDataDir;
	}

	/**
	 * Main method
	 */
	public AnalysisReply handleRequest(AnalysisRequest request)
	{
		LOGGER.debug("handleRequest");

		AnalysisReply reply = null;

		analysisArguments = request.getAnalysisArguments();

		UUID requestId = request.getRequestId();
		String requestIdStr = requestId.toString();
		String obsCsvUrlStr = (String) analysisArguments.get(OBS_CSV);
		String userName = (String) analysisArguments.get(USER_NAME);
		String dataSetName = (String) analysisArguments.get(DATA_SET_NAME);
		String dataSetVersionName = (String) analysisArguments
				.get(DATA_SET_VERSION_NAME);
		String longitude = (String) analysisArguments.get(LONGITUDE);
		String latitude = (String) analysisArguments.get(LATITUDE);
		String elevation = (String) analysisArguments.get(ELEVATION);
		String towerHeight = (String) analysisArguments.get(TOWER_HEIGHT);

		URL remoteFileUrl;
		File csvLocalFile = null;
		Map<String, File> outputLocalFiles = null;
		URL storeUrl = null;
		Map<String, URL> remoteOutputFileUrls = null;
		try
		{
			remoteFileUrl = new URL(obsCsvUrlStr);
			String csvLocalFileName = requestIdStr + CVS_FILE_SUFFIX;
			csvLocalFile = new File(inputDataDir, csvLocalFileName);
			copyRemoteFileToLocal(remoteFileUrl, csvLocalFile);

			outputLocalFiles = convertCSV2NetCDF(csvLocalFile,
					this.outputDataDir, requestIdStr, userName, dataSetName,
					dataSetVersionName, longitude, latitude, elevation,
					towerHeight);

			// clean up when all succeeded
			deleteLocalFile(csvLocalFile);
			// store away output files
			storeUrl = getStoreUrl(remoteFileUrl); // use the same store
			remoteOutputFileUrls = putFilesIntoStore(storeUrl, outputLocalFiles);
			deleteLocalFiles(outputLocalFiles);

			reply = makeNormReply(requestId, remoteOutputFileUrls);
		}
		catch (MalformedURLException e)
		{
			LOGGER.warn(e);
			reply = makeErrorReply(requestId, e);
		}
		catch (AnalysisException e)
		{
			LOGGER.warn(e);
			reply = makeErrorReply(requestId, e);
		}
		catch (IOException e)
		{
			LOGGER.warn(e);
			reply = makeErrorReply(requestId, e);
		}

		return reply;
	}

	/**
	 * A helper class to pack up NORMAL reply
	 * 
	 * @param requestId
	 * @param remoteOutputFileUrls
	 * @return
	 */
	private AnalysisReply makeNormReply(UUID requestId,
			Map<String, URL> remoteOutputFileUrls)
	{
		AnalysisReply reply = new AnalysisReply(requestId);
		reply.setStatus(AnalysisReply.Status.NORMAL);
		Map<String, Object> returnMap = new HashMap<String, Object>();
		for (String key : remoteOutputFileUrls.keySet())
		{
			Object value = (Object) remoteOutputFileUrls.get(key);
			returnMap.put(key, value);
		}
		reply.setAnalysisResults((Map<String, Object>) returnMap);
		return reply;
	}

	/**
	 * A helper class to pack up ERROR reply
	 * 
	 * @param requestId
	 * @param e
	 * @return
	 */
	private AnalysisReply makeErrorReply(UUID requestId, Exception e)
	{
		AnalysisReply reply = new AnalysisReply(requestId);
		reply.setStatus(AnalysisReply.Status.ERROR);

		return reply;
	}

	/**
	 * Copy a remote file to a local file
	 * @param remoteFileUrl
	 * @param localFile
	 * @throws IOException
	 */
	private void copyRemoteFileToLocal(URL remoteFileUrl, File localFile)
			throws IOException
	{
		String protocol = remoteFileUrl.getProtocol();
		RemoteFileHandler remoteFileHandler = UrlProtocolHandlerFactory
				.getHandler(protocol);
		remoteFileHandler.copyRemoteFileToLocal(remoteFileUrl, localFile);
	}

	/**
	 * Get the remote store URL
	 * @param remoteFileUrl
	 * @return
	 */
	private URL getStoreUrl(URL remoteFileUrl)
	{
		String protocol = remoteFileUrl.getProtocol();
		RemoteFileHandler remoteFileHandler = UrlProtocolHandlerFactory
				.getHandler(protocol);
		URL remoreStoreUrl = remoteFileHandler.getStoreUrl(remoteFileUrl);
		return remoreStoreUrl;
	}

	/**
	 * Copy from local files to remote files
	 * 
	 * @param storeUrl
	 * @param outputFileLocalUrls
	 * @return
	 * @throws IOException 
	 */
	private Map<String, URL> putFilesIntoStore(URL storeUrl,
			Map<String, File> outputFileLocalFiles)
			throws IOException
	{
		String protocol = storeUrl.getProtocol();
		RemoteFileHandler remoteFileHandler = UrlProtocolHandlerFactory
				.getHandler(protocol);

		// create remote file URLs
		Map<String, URL> remoteUrls = new HashMap<String, URL>();
		String remoteDirStr = storeUrl.toExternalForm();
		URL remoteFileUrl = null;
		File localFile = null;
		String localFileName = null;
		for (String key : outputFileLocalFiles.keySet())
		{
			localFile = outputFileLocalFiles.get(key);
			localFileName = localFile.getName();
			remoteFileUrl = new URL(remoteDirStr + "/" + localFileName);
			remoteUrls.put(key, remoteFileUrl);
		}

		// Then, let the handler do the copying
		remoteFileHandler.storeFilesIntoStore(outputFileLocalFiles, remoteUrls);
		return remoteUrls;
	}

	/**
	 * It deletes multiple files. It is just the normal file delete but it
	 * throws an exception
	 * 
	 * @param outputFileLocalFiles
	 * @throws AnalysisException
	 */
	private void deleteLocalFiles(Map<String, File> outputFileLocalFiles)
			throws AnalysisException
	{
		for (File f : outputFileLocalFiles.values())
		{
			deleteLocalFile(f);
		}
	}

	/**
	 * Just a normal file delete but it throws an exception
	 * 
	 * @param csvFileLocalFile
	 * @throws AnalysisException
	 */
	private void deleteLocalFile(File csvFileLocalFile)
			throws AnalysisException
	{
		boolean isDeleted = csvFileLocalFile.delete();
		if (!isDeleted) throw new AnalysisException("can't delete: "
				+ csvFileLocalFile);
	}

	/**
	 * The role of this method is to run the analysisEngine. The analysisEngine
	 * runs the analysis implementation, which will read the localCSVFileURL and
	 * try to output outcome to specified URLs. It expects the engine to throw
	 * an exception if it fails to produce output files.
	 * 
	 * @param localCSVFile
	 * @param outputDataDir
	 * @param requestIdStr
	 * @param userName
	 * @param dataSetName
	 * @param dataSetVersionName
	 * @param longitude
	 * @param latitude
	 * @param elevation
	 * @param towerHeight
	 * @return
	 * @throws AnalysisException
	 */
	private Map<String, File> convertCSV2NetCDF(File localCSVFile,
			File outputDataDir, String requestIdStr, String userName,
			String dataSetName, String dataSetVersionName, String longitude,
			String latitude, String elevation, String towerHeight)
			throws AnalysisException
	{
		LOGGER.debug("convertCSV2NetCDF()");

		File fluxNetCDFFile = new File(outputDataDir, requestIdStr
				+ FLUX_FILE_SUFFIX);
		File metNetCDFFile = new File(outputDataDir, requestIdStr
				+ MET_FILE_SUFFIX);

		/*
		 * The engine should be set in IoC so it does not have to know its
		 * implementation or be created here with R_LIBS_USER
		 */
		if (this.csv2NetCDFDao == null) this.csv2NetCDFDao = new CSV2NetCDFDao(
				this.palsRserveEngine);

		Map<String, File> outputNetCDFFiles = this.csv2NetCDFDao
				.convertCSV2NetCDF(localCSVFile, fluxNetCDFFile, metNetCDFFile,
						userName, dataSetName, dataSetVersionName, longitude,
						latitude, elevation, towerHeight);

		return outputNetCDFFiles;
	}
	
	public CSV2NetCDFDao getCsv2NetCDFDao()
	{
		return csv2NetCDFDao;
	}

	public void setCsv2NetCDFDao(CSV2NetCDFDao csv2NetCDFDao)
	{
		this.csv2NetCDFDao = csv2NetCDFDao;
	}
}
