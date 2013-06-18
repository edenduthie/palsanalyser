package org.pals.analysis.analyser.handler;

import java.io.File;

import org.pals.analysis.analyser.handler.dao.PalsRserveEngine;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

public class BenchPlotHandler implements RequestHandler
{
	private String inputDataDirPath;
	private String outputDataDirPath;

	public BenchPlotHandler(PalsRserveEngine palsRserveEngine,
			File inputDataDir, File outputDataDir)
	{
		// TODO Auto-generated constructor stub
	}

	public AnalysisReply handleRequest(AnalysisRequest requests)
			throws AnalysisException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
