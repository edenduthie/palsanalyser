package org.pals.analysis.analyser.handler.engine;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.request.AnalysisException;

/**
 * This class provides 'renjin" based interface between Java and the
 * implementation of analysis, which is written in R script.
 * 
 * It expects the R analysis to be written as an R function.
 * 
 * It sets R variables using the arguments passed by Java. Then it calls an R
 * function by using the variables as arguments.
 * 
 * TODO: The function should be pre-loaded. It should be "installed" as library,
 * using R CMD INSTALL ..., because it is a once-off interpretation and
 * compilation.
 * 
 * @author Yoichi
 * 
 */
public class CSV2NetCDFEngine extends AnalysisREngine
{
	public Map<String, Object> convertCSV2NetCDF(URL ovsCSVURL, URL obsFluxURL,
			URL obsMetURL, String userName, String dataSetName,
			String dataSetVersionName, String longitude, String latitude,
			String elevation, String towerHeight) throws AnalysisException
	{
		Map<String, Object> result = null;

		String rSentence = null;
		try
		{
			rSentence = CSV2NetCDFHandler.OBS_CSV + "=\""
					+ ovsCSVURL.toExternalForm() + "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.OBS_FLUX + "=\""
					+ obsFluxURL.toExternalForm() + "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.OBS_MET + "=\""
					+ obsMetURL.toExternalForm() + "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.USER_NAME + "=\"" + userName + "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.DATA_SET_NAME + "=\"" + dataSetName
					+ "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.DATA_SET_VERSION_NAME + "=\""
					+ dataSetVersionName + "\"";
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.LONGITUDE + "=" + longitude;
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.LATITUDE + "=" + latitude;
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.ELEVATION + "=" + elevation;
			engine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.TOWER_HEIGHT + "=" + towerHeight;
			engine.eval(rSentence);
			rSentence = "result<-" + CSV2NetCDFHandler.FUNCTION_NAME + "("
					+ CSV2NetCDFHandler.OBS_CSV + ","
					+ CSV2NetCDFHandler.OBS_FLUX + ","
					+ CSV2NetCDFHandler.OBS_MET + ","
					+ CSV2NetCDFHandler.USER_NAME + ","
					+ CSV2NetCDFHandler.DATA_SET_NAME + ","
					+ CSV2NetCDFHandler.DATA_SET_VERSION_NAME + ","
					+ CSV2NetCDFHandler.LONGITUDE + ","
					+ CSV2NetCDFHandler.LATITUDE + ","
					+ CSV2NetCDFHandler.ELEVATION + ","
					+ CSV2NetCDFHandler.TOWER_HEIGHT + ")";
			engine.eval(rSentence);
			/*
			 * It expects R to have thrown an exception if it failed to create
			 * files. So, this engine just uses the input URLs as outputs
			 * assuming it has succeeded.
			 */
			result = new HashMap<String, Object>();
			result.put(CSV2NetCDFHandler.OBS_FLUX, obsFluxURL);
			result.put(CSV2NetCDFHandler.OBS_MET, obsMetURL);
		}
		catch (ScriptException e)
		{
			throw new AnalysisException(e);
		}
		return result;
	}
}
