package org.pals.analysis.analyser.handler;

import java.io.File;

import org.pals.analysis.analyser.handler.dao.PalsRserveEngine;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

public class EmpBenchmarkHandler implements RequestHandler
{
	private String inputDataDirPath;
	private String outputDataDirPath;
	private PalsRserveEngine palsRserveEngine;

	public EmpBenchmarkHandler(PalsRserveEngine palsRserveEngine,
			File inputDataDir, File outputDataDir)
	{
		this.palsRserveEngine = palsRserveEngine;
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
