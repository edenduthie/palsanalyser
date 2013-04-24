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
import java.util.logging.Logger;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

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

	/**
	 * The method retrieves the remote file and parameters and pass them to the
	 * analysis method
	 */
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
		File localCSVFile = null;
		Map<String, Object> outputFileURLs = null;
		try
		{
			remoteFileURL = new URL(obsCsvURLStr);
			String protocol = remoteFileURL.getProtocol();
			/**
			 * Currently, it handles only file:// URLs
			 */
			if (FILE_PROTOCOL.equals(protocol)) localCSVFile = getFileViaFileProtocol(
					requestIdStr, this.inputDataDirPath, remoteFileURL);

			else
				throw new AnalysisException("unknown protocol: " + protocol);

			outputFileURLs = convertCSV2NetCDFWithR(localCSVFile,
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
	private Map<String, Object> convertCSV2NetCDFWithR(File localCSVFile,
			String userName, String dataSetName, String dataSetVersionName,
			String longitude2, String latitude2, String elevation2,
			String towerHeight, String towerHeight2)
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * This copies the remote file to a local file and deletes the remote file.
	 * TODO: If both files are on the local fs, the remote just needs to be
	 * moved/renamed.
	 * 
	 * @param requestId
	 * @param inputDataDirPath
	 * @param remoteFileURL
	 * @return
	 * @throws IOException
	 */
	private File getFileViaFileProtocol(String requestId,
			String inputDataDirPath, URL remoteFileURL) throws IOException
	{
		String remoteFilePathStr = remoteFileURL.getPath();
		String newFilePathStr = inputDataDirPath + File.separator + requestId
				+ ".csv";
		File remoteFile = new File(remoteFilePathStr);
		File newFile = new File(newFilePathStr);

		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		boolean isDeleted = false;
		try
		{
			inputChannel = new FileInputStream(remoteFile).getChannel();
			outputChannel = new FileOutputStream(newFile).getChannel();

			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());

			isDeleted = remoteFile.delete();
			if (!isDeleted) LOGGER.warning("cound not delete: "
					+ remoteFile.toURI());
		}
		finally
		{
			inputChannel.close();
			outputChannel.close();
		}

		return newFile;
	}
}
