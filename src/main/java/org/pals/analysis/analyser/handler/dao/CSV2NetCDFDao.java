package org.pals.analysis.analyser.handler.dao;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.rabbitmq.AnalysisServlet;
import org.pals.analysis.request.AnalysisException;

/**
 * This class is a data exchange layer between Java and R.
 * 
 * convertCSV2NetCDF() is translated to an R function call and executed.
 * 
 * @author Yoichi
 * 
 */
public class CSV2NetCDFDao
{
	private final static Logger LOGGER = Logger.getLogger(CSV2NetCDFDao.class
			.getName());
	private final static String PALS_R_PACKAGE = "pals";

	private PalsREngine palsREngine;
	
	/**
	 * TODO: Set the engine via IoC, than in this constructor.
	 * @param palsREngine 
	 * @throws ScriptException 
	 */
	public CSV2NetCDFDao(PalsREngine palsREngine) throws ScriptException
	{
		this.palsREngine = palsREngine;
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
			rSentence = "library("+ PALS_R_PACKAGE + ")";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.OBS_CSV + "=\""
					+ ovsCSVURL.toExternalForm() + "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.OBS_FLUX + "=\""
					+ obsFluxURL.toExternalForm() + "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.OBS_MET + "=\""
					+ obsMetURL.toExternalForm() + "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.USER_NAME + "=\"" + userName + "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			rSentence = CSV2NetCDFHandler.DATA_SET_NAME + "=\"" + dataSetName
					+ "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.DATA_SET_VERSION_NAME + "=\""
					+ dataSetVersionName + "\"";
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.LONGITUDE + "=" + longitude;
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.LATITUDE + "=" + latitude;
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.ELEVATION + "=" + elevation;
			LOGGER.debug(rSentence);
			palsREngine.eval(rSentence);
			
			rSentence = CSV2NetCDFHandler.TOWER_HEIGHT + "=" + towerHeight;
			LOGGER.debug(rSentence);
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
			LOGGER.debug(rSentence);
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
