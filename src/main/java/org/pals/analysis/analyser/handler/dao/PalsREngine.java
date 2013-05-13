package org.pals.analysis.analyser.handler.dao;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * PALS analysis engine.
 * 
 * It encapsulates the instance of Renjin with initialisation.
 * 
 * It specifies $R_LIBS_USER for R engine. $R_LIBS_USER is where all extra R
 * packages are "installed".
 * 
 * It also allows the user to specify required package name.
 * 
 * TODO: Each Worker instance probably should keep an instance of this engine.
 * TODO: Package method should be load/unload, rather than set/get.
 * 
 * @author Yoichi
 * 
 */
public class PalsREngine
{
	private ScriptEngineManager factory;
	private ScriptEngine engine;
	private final static String R_LIBS_USER = "/root/workspace";
	private final static String PALSAPI_PKG = "palsapi";
	private String rLibsUserPath = R_LIBS_USER;
	private String palsAPIPkgName = PALSAPI_PKG;

	/**
	 * Creating a script engine is an expensive step
	 * @throws ScriptException
	 */
	public PalsREngine() throws ScriptException
	{
		this.factory = new ScriptEngineManager();
		this.engine = factory.getEngineByName("Renjin");
		setRLibsUserPath(null);
		setPalsAPIPkgName(null);
	}

	public String getRLibsUserPath()
	{
		return rLibsUserPath;
	}

	public void setRLibsUserPath(String rLibsUserPath) throws ScriptException
	{
		if (rLibsUserPath == null || rLibsUserPath == "") rLibsUserPath = R_LIBS_USER;

		String rStatement = "Sys.setenv(R_LIBS_USER = \"" + rLibsUserPath
				+ "\")";
		this.engine.eval(rStatement);
		this.rLibsUserPath = rLibsUserPath;
	}

	public String getPalsAPIPkgName()
	{
		return palsAPIPkgName;
	}

	public void setPalsAPIPkgName(String palsAPIPkgName) throws ScriptException
	{
		if (palsAPIPkgName == null || palsAPIPkgName == "")
		{
			palsAPIPkgName = PALSAPI_PKG;
		}

		String rStatement;
		 rStatement = "library('plotrix',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		 rStatement = "library('pals',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		 rStatement = "library('" + palsAPIPkgName + "',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		this.palsAPIPkgName = palsAPIPkgName;
	}

	/**
	 * TODO: In ScriptEngine eval() is overloaded
	 * 
	 * @param statement
	 * @return
	 * @throws ScriptException
	 */
	public Object eval(String statement) throws ScriptException
	{
		Object result = this.engine.eval(statement);
		return result;
	}
}
