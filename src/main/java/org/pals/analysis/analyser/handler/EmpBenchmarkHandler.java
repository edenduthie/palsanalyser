package org.pals.analysis.analyser.handler;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

public class EmpBenchmarkHandler implements RequestHandler
{
	private String inputDataDirPath;
	private String outputDataDirPath;

	public EmpBenchmarkHandler(String inputDataDirPath, String outputDataDirPath)
	{
		this.inputDataDirPath = inputDataDirPath;
		this.outputDataDirPath = outputDataDirPath;
	}

	public AnalysisReply handleRequest(AnalysisRequest request)
			throws AnalysisException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
