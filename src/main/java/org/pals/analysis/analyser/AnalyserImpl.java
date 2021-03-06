package org.pals.analysis.analyser;

import java.io.File;

import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.pals.analysis.analyser.handler.BenchPlotHandler;
import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.analyser.handler.EmpBenchmarkHandler;
import org.pals.analysis.analyser.handler.ModelPlotHandler;
import org.pals.analysis.analyser.handler.ObsPlotHandler;
import org.pals.analysis.analyser.handler.QCPlotHandler;
import org.pals.analysis.analyser.handler.dao.PalsREngine;
import org.pals.analysis.analyser.handler.dao.PalsRserveEngine;
import org.pals.analysis.rabbitmq.AnalysisServlet;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * The analyser is a facade to deal with all analysis requests. It provides a
 * switchboard to branch out to appropriate handlers. Currently the handlers are
 * hard-wired in this class. In the future, they may be dynamically wired as
 * beans. Interfaces may be used to allow dynamic wiring of handler
 * implementations.
 * 
 * All analysis requests could be passed to R server by a uniform way, e.g.,
 * generating R statements to set arguments as R variables and then calling the
 * a function to use them, without using Handlers. Handlers are needed, however,
 * because there may be multiple outcome variables in the R scripts and Handlers
 * will know what to package up in the replies.
 * 
 * Note 1: Empirical benchmark is produced as asynchronous request. Note 2:
 * OBS*, MODEL* and BENCH* can be requested as asynchronous requests.
 * 
 * @author Yoichi
 */
public class AnalyserImpl implements Analyser
{
	private final static Logger LOGGER = Logger.getLogger(AnalysisServlet.class
			.getName());

	// PalsREngine should be created only once per analyzer
	private PalsRserveEngine palsRserveEngine = null;

	public AnalysisReply analyse(AnalysisRequest request, File inputDataDir,
			File outputDataDir) throws AnalysisException
	{
		if (this.palsRserveEngine == null)

		this.palsRserveEngine = new PalsRserveEngine();

		AnalysisReply reply = null;
		String analysisName = request.getAnalysisName();
		if (AnalysisRequest.CVS2NETCDF.equals(analysisName))
		{
			CSV2NetCDFHandler handler = new CSV2NetCDFHandler(
					this.palsRserveEngine, inputDataDir, outputDataDir);

			LOGGER.debug("calling CSV2NetCDFHandler");
			reply = handler.handleRequest(request);
			LOGGER.debug("returned from CSV2NetCDFHandler");
		}
		else
			if (AnalysisRequest.QCPLOT.equals(analysisName))
			{
				QCPlotHandler handler = new QCPlotHandler(
						this.palsRserveEngine, inputDataDir, outputDataDir);
				reply = handler.handleRequest(request);
			}
			else
				if (AnalysisRequest.EMPBENCH.equals(analysisName))
				{
					EmpBenchmarkHandler handler = new EmpBenchmarkHandler(
							this.palsRserveEngine, inputDataDir, outputDataDir);
					reply = handler.handleRequest(request);
				}
				else
					if (analysisName.startsWith(AnalysisRequest.OBS))
					{
						ObsPlotHandler handler = new ObsPlotHandler(
								this.palsRserveEngine,
								inputDataDir, outputDataDir);
						reply = handler.handleRequest(request);
					}
					else
						if (analysisName.startsWith(AnalysisRequest.MODEL))
						{
							ModelPlotHandler handler = new ModelPlotHandler(
									this.palsRserveEngine, inputDataDir,
									outputDataDir);
							reply = handler.handleRequest(request);
						}
						else
							if (analysisName.startsWith(AnalysisRequest.BENCH))
							{
								BenchPlotHandler handler = new BenchPlotHandler(
										this.palsRserveEngine, inputDataDir,
										outputDataDir);
								reply = handler.handleRequest(request);
							}
							else
							{
								String msg = "unrecognized analysisName: "
										+ analysisName;
								AnalysisException e = new AnalysisException(msg);
								throw e;
							}

		return reply;
	}
}
