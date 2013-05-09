package org.pals.analysis.analyser.handler.dao;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.request.AnalysisException;

/**
 * This class is a data exchange layer between Java and R.
 * 
 * convertCSV2NetCDF is translated to R syntax.
 * 
 * @author Yoichi
 * 
 */
public class CSV2NetCDFDao
{
	private PalsREngine palsREngine;
	
	/**
	 * TODO: Set the engine via IoC, than in this constructor.
	 * @param palsREngine 
	 * @throws ScriptException 
	 */
	public CSV2NetCDFDao(PalsREngine palsREngine) throws ScriptException
	{
		this.palsREngine = new PalsREngine();
	}
	
	public Map<String, Object> convertCSV2NetCDF(URL ovsCSVURL, URL obsFluxURL,
			URL obsMetURL, String userName, String dataSetName,
			String dataSetVersionName, String longitude, String latitude,
			String elevation, String towerHeight) throws AnalysisException
	{
		Map<String, Object> result = null;

		String rSentence = null;
		try
		{
			palsREngine.eval(rSentence);			
			rSentence = "library(palsapi)"; // this is not needed but just in case
			palsREngine.eval(rSentence);			
			rSentence = CSV2NetCDFHandler.OBS_CSV + "=\""
					+ ovsCSVURL.toExternalForm() + "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.OBS_FLUX + "=\""
					+ obsFluxURL.toExternalForm() + "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.OBS_MET + "=\""
					+ obsMetURL.toExternalForm() + "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.USER_NAME + "=\"" + userName + "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.DATA_SET_NAME + "=\"" + dataSetName
					+ "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.DATA_SET_VERSION_NAME + "=\""
					+ dataSetVersionName + "\"";
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.LONGITUDE + "=" + longitude;
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.LATITUDE + "=" + latitude;
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.ELEVATION + "=" + elevation;
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.TOWER_HEIGHT + "=" + towerHeight;
			palsREngine.eval(rSentence);
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
			palsREngine.eval(rSentence);
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

	public PalsREngine getEngine()
	{
		return palsREngine;
	}

	public void setEngine(PalsREngine engine)
	{
		this.palsREngine = engine;
	}
}
