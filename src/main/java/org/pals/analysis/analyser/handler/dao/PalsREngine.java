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
 * @deprecated
 */
public class PalsREngine
{
	private ScriptEngineManager factory;
	private ScriptEngine engine;
	private final static String R_LIBS_USER = "/root/workspace";
	private final static String PALS_PKG = "pals";
	private String rLibsUserPath = R_LIBS_USER;
	private String palsPkgName = PALS_PKG;

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
		return palsPkgName;
	}

	public void setPalsAPIPkgName(String palsPkgName) throws ScriptException
	{
		if (palsPkgName == null || palsPkgName == "")
		{
			palsPkgName = PALS_PKG;
		}

		String rStatement;
		 rStatement = "library('plotrix',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		 rStatement = "library('pals',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		 rStatement = "library('" + palsPkgName + "',lib.loc='" + R_LIBS_USER + "')";
		this.engine.eval(rStatement);
		this.palsPkgName = palsPkgName;
	}
}
