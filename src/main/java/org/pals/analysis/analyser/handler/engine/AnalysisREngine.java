package org.pals.analysis.analyser.handler.engine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
/**
 * Super class of analysis engine classes.
 * 
 * It provides a chance to specify user library location for all engine subclasses.
 * User library is where all extra R packages can be "installed".
 * 
 * @author Yoichi
 *
 */
public abstract class AnalysisREngine 
{
	protected ScriptEngineManager factory;
	protected ScriptEngine engine;

	public AnalysisREngine()
	{
		this.factory = new ScriptEngineManager();
		this.engine = factory.getEngineByName("Renjin");
	}
}
