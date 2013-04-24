package org.pals.analysis.request;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author Yoichi
 * 
 */
public class AnalysisRequest
{
	public static enum AnalysisType
	{
		CVS2NETCDF, QCPLOT, EMPBENCHMARK, OBSERVED, MODELOUTPUT, BENCH
	};

	public static final String CVS2NETCDF = "CVS2NETCDF";
	public static final String QCPLOT = "QCPLOT";
	public static final String EMPBENCH = "EMPBENCH";
	public static final String OBS = "OBS";
	public static final String MODEL = "MODEL";
	public static final String BENCH = "BENCH";
	private UUID requestId;
	private String analysisName;
	private Map<String, Object> analysisArguments;

	public AnalysisRequest()
	{
		this(null, null);
	}

	public AnalysisRequest(String analysisName,
			Map<String, Object> analysisArguments)
	{
		Date today = new Date();
		String todayString = today.toString();
		byte[] name = todayString.getBytes();
		requestId = UUID.nameUUIDFromBytes(name);
		this.analysisName = analysisName;
		this.analysisArguments = analysisArguments;
	}

	public String getAnalysisName()
	{
		return analysisName;
	}

	public void setAnalysisName(String analysisName)
	{
		this.analysisName = analysisName;
	}

	public Map<String, Object> getAnalysisArguments()
	{
		return analysisArguments;
	}

	public void setAnalysisArguments(Map<String, Object> analysisArguments)
	{
		this.analysisArguments = analysisArguments;
	}

	public UUID getRequestId()
	{
		return requestId;
	}
}
