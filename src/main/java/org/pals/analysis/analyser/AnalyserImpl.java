package org.pals.analysis.analyser;

import org.pals.analysis.analyser.handler.BenchPlotHandler;
import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.analyser.handler.ModelPlotHandler;
import org.pals.analysis.analyser.handler.ObsPlotHandler;
import org.pals.analysis.analyser.handler.QCPlotHandler;
import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * The analyser is a facado to deal with all analysis requests. It provides a
 * switchboard to branch out to appropriate handlers. Interfaces may be used to
 * allow dynamic wiring of handler implementations. Note 1: Empirical benchmark
 * is produced as asynchronous request. Note 2: OBS*, MODEL* and BENCH* can be
 * requested as asynchronous requests.
 * 
 * @author Yoichi
 */
public class AnalyserImpl implements Analyser
{
	public AnalysisReply analyse(AnalysisRequest request)
			throws AnalysisException
	{
		AnalysisReply reply = null;
		String analysisName = request.getAnalysisName();
		if (AnalysisRequest.CVS2NETCDF.equals(analysisName))
		{
			CSV2NetCDFHandler handler = new CSV2NetCDFHandler();
			reply = handler.handleRequest(request);
		}
		else
			if (AnalysisRequest.QCPLOT.equals(analysisName))
			{
				QCPlotHandler handler = new QCPlotHandler();
				reply = handler.handleRequest(request);
			}
			else
				if (analysisName.startsWith(AnalysisRequest.OBS))
				{
					ObsPlotHandler handler = new ObsPlotHandler();
					reply = handler.handleRequest(request);
				}
				else
					if (analysisName.startsWith(AnalysisRequest.MODEL))
					{
						ModelPlotHandler handler = new ModelPlotHandler();
						reply = handler.handleRequest(request);
					}
					else
						if (analysisName.startsWith(AnalysisRequest.BENCH))
						{
							BenchPlotHandler handler = new BenchPlotHandler();
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
