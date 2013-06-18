package org.pals.analysis.analyser.handler.dao;

import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.pals.analysis.request.AnalysisException;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

/**
 * PALS analysis engine using Rserve connection.
 * 
 * {@link http://rosuda.org/Rserve/doc.shtml}
 * 
 * It specifies $R_LIBS_USER for R engine. $R_LIBS_USER is where all extra R
 * packages are "installed".
 * 
 * It also allows the user to specify required package name.
 * 
 * Rserve process must be running on OS in order for this code to be able to
 * connect to R.
 * 
 * <pre>
 * <code>
 * $ R
 * > install.packages('Rserve')
 * > q()
 * </code> 
 * Or, e.g.,
 * <code> $ R CMD INSTALL Rserve_1.7-0.tar.gz </code>
 * Then 
 * <code>
 * $ sudo -u nobody R CMD Rserve --gui-none --RS-port 16311
 * </code>
 * </pre>
 * 
 * Never run Rserve as "root". Rserve is run at the startup time as a daemon on
 * Linux using start-stop-daemon. The config can also be set in /etc/RServ.conf
 * on Linux.
 * 
 * @author Yoichi
 * 
 *         TODO: Each Worker instance probably should keep an instance of this
 *         engine. TODO: Package method should be load/unload, rather than
 *         set/get.
 */
public class PalsRserveEngine
{
	private final static Logger LOGGER = Logger
			.getLogger(PalsRserveEngine.class.getName());

	private final static String HOST = "localhost";
	private final static int PORT = 16311;
	private final static String DISPLAY = "localhost:100.0";
	private String host = HOST;
	private int port = PORT;
	private String display = DISPLAY;
	private final static String R_LIBS_USER = "/root/workspace";
	private final static String PALS_PKG = "pals";
	private RConnection connection;
	private String rLibsUserPath = R_LIBS_USER;
	private String palsPkgName = PALS_PKG;

	/**
	 * Creating a script engine is an expensive step
	 * 
	 * @throws AnalysisException
	 */
	public PalsRserveEngine() throws AnalysisException
	{
		this(HOST, PORT, DISPLAY);
	}

	public PalsRserveEngine(String host, int port, String display)
			throws AnalysisException
	{
		this.host = host;
		this.port = port;
		this.display = display;
		try
		{
			this.connection = new RConnection(host, port);
		}
		catch (RserveException e)
		{
			throw new AnalysisException(e);
		}
		boolean isConnected = this.connection.isConnected();
		if (!isConnected) throw new AnalysisException(
				"REngine is not connected");
		setDisplay(display);
		setRLibsUserPath(null);
		setPalsAPIPkgName(null);
	}

	public void setDisplay(String display) throws AnalysisException
	{
		this.display = display;
		String setDisplay = "Sys.setenv(\"DISPLAY\"=\"" + display + "\")";

		LOGGER.debug(setDisplay);

		try
		{
			this.connection.eval(setDisplay);
		}
		catch (RserveException e)
		{
			throw new AnalysisException(e);
		}
	}

	public String getDisplay() throws AnalysisException
	{
		String getDisplay = "Sys.getenv(c(\"DISPLAY\"))";
		LOGGER.debug(getDisplay);

		String display;
		try
		{
			display = this.connection.eval(getDisplay).asString();
		}
		catch (RserveException e)
		{
			throw new AnalysisException(e);
		}
		catch (REXPMismatchException e)
		{
			throw new AnalysisException(e);
		}
		this.display = display;
		return display;
	}

	public String getRLibsUserPath()
	{
		return rLibsUserPath;
	}

	public void setRLibsUserPath(String rLibsUserPath) throws AnalysisException
	{
		if (rLibsUserPath == null || rLibsUserPath == "") rLibsUserPath = R_LIBS_USER;

		String rStatement = "Sys.setenv(\"R_LIBS_USER\"=\"" + rLibsUserPath
				+ "\")";
		LOGGER.debug(rStatement);

		try
		{
			this.connection.eval(rStatement);
		}
		catch (RserveException e)
		{
			throw new AnalysisException(e);
		}
		this.rLibsUserPath = rLibsUserPath;
	}

	public String getPalsAPIPkgName()
	{
		return palsPkgName;
	}

	public void setPalsAPIPkgName(String palsPkgName) throws AnalysisException
	{
		if (palsPkgName == null || palsPkgName == "")
		{
			palsPkgName = PALS_PKG;
		}

		String rStatement;
		try
		{
			rStatement = "library(plotrix)";
			LOGGER.debug(rStatement);
			this.connection.eval(rStatement);
			rStatement = "library(pals)";
			LOGGER.debug(rStatement);
			this.connection.eval(rStatement);
			rStatement = "library(" + palsPkgName + "')";
			LOGGER.debug(rStatement);
			this.connection.eval(rStatement);
		}
		catch (RserveException e)
		{
			throw new AnalysisException(e);
		}

		this.palsPkgName = palsPkgName;
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public RConnection getConnection()
	{
		return connection;
	}
}
