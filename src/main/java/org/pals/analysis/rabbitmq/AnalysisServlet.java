package org.pals.analysis.rabbitmq;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;

/**
 * A RabbitMQ version of the analysis server to start running workers. It can be
 * run as (1) a stand-alone Java application via main(), (2) instantiated as a
 * class where workers are run by init(), or (3) run as a Servlet. All the cases
 * use init() to instantiate workers and run them as threads. The purpose of the
 * server is to hold both the threads and workers in JVM, until all the
 * executions finish. When it is run as a Servlet, get/put methods can be added
 * to control or check the status of the workers or threads. Once the worker
 * threads are registered with RabbiMQ and run, workers receive messages via the
 * RabbitMQ Channel (queue), not as HttpServletRequest via the Servlet API.
 * Therefore, this server can be run in the same JVM as that runs clients or in
 * an independent JVM. Clients should send messages to the Channel directly. In
 * addition, the RabbitMQ server should be installed on the localhost and be
 * running. Note: The number of workers is defined in the web.xml. TODO: The
 * number of workers may be changed at runtime. TODO: This class should be a
 * singleton, unless queue name can change per instance.
 * 
 * @author Yoichi
 * 
 */
public class AnalysisServlet extends HttpServlet
{
	private final static Logger LOGGER = Logger.getLogger(AnalysisServlet.class
			.getName());
	private static final long serialVersionUID = -1975361388372623587L;
	private static long SLEEP_DURATION = 10000;

	private boolean isRunningAsMain = false;
	private boolean isRunningAsServlet = true;

	// default values
	// TODO: These may be set by using IoC
	private String rpcQueueName = "pals_analysis";
	private String inputDataDirPath = "/tmp/palsAnalyser/input";
	private String outputDataDirPath = "/tmp/palsAnalyser/output";
	private int numOfWorkers = 4;

	private List<Thread> threads;
	private List<AnalysisWorker> workers;

	/**
	 * Main: It uses init() and destroy() of Servlet methods
	 * 
	 * @param argv
	 */
	public static void main(String[] argv)
	{
		AnalysisServlet me = new AnalysisServlet();
		me.isRunningAsMain = true;
		me.isRunningAsServlet = false;
		try
		{
			me.init();

			// It must keep running if it is not a Servlet
			while (me.isRunningAsMain)
			{
				try
				{
					Thread.sleep(SLEEP_DURATION);
				}
				catch (InterruptedException e)
				{
					LOGGER.info("server is to terminate");
					me.isRunningAsMain = false;
				}
			}
		}
		catch (ServletException e)
		{
			e.printStackTrace();
		}
		finally
		{
			me.destroy();
		}
	}

	public void stopMain()
	{
		this.isRunningAsMain = false;
	}

	/**
	 * Constructor
	 */
	public AnalysisServlet()
	{
		this.threads = new ArrayList<Thread>();
		this.workers = new ArrayList<AnalysisWorker>();
	}

	@Override
	public void init() throws ServletException
	{
		if (this.threads == null) this.threads = new ArrayList<Thread>();
		if (this.workers == null) this.workers = new ArrayList<AnalysisWorker>();

		for (int i = 0; i < this.numOfWorkers; i++)
		{
			// worker is a Runnable
			String workerId = String.valueOf(i);
			AnalysisWorker worker = new AnalysisWorker(workerId, rpcQueueName,
					inputDataDirPath, outputDataDirPath);
			workers.add(worker);
			worker.init();
			// Create a new thread with the worker
			Thread thread = new Thread(worker, workerId);
			thread.start();
			threads.add(thread);
			LOGGER.info("workerId=" + workerId + " created and started");
		}

		super.init();
	}

	@Override
	public void destroy()
	{
		if (this.isRunningAsMain) stopMain();

		for (Object o : workers)
		{
			AnalysisWorker worker = (AnalysisWorker) o;
			String workerId = worker.getWorkerId();
			LOGGER.info("[worker " + workerId + "] calling destroy()");
			worker.destroy(); // this sets isRunning to false
			worker = null;
		}
		workers.clear(); // same as above
		workers = null;
		LOGGER.info("references to all workers are removed");
		for (Object o : threads)
		{
			Thread thread = (Thread) o;
			String threadName = thread.getName();
			/**
			 * threads should have terminated normally. If they have not,
			 * however, it may be necessary to interrupt them
			 */
			thread.interrupt(); // stop the sleeping loop so the thread can
								// terminate as normal
			thread = null;
			LOGGER.info("[thread " + threadName
					+ "] thread interrupted and made to null");
		}
		threads.clear(); // same as above
		threads = null;
		LOGGER.info("references to all threads are removed");

		super.destroy();
	}

	public boolean isRunningAsMain()
	{
		return isRunningAsMain;
	}

	public void setRunningAsMain(boolean isRunningAsMain)
	{
		this.isRunningAsMain = isRunningAsMain;
	}

	public boolean isRunningAsServlet()
	{
		return isRunningAsServlet;
	}

	public void setRunningAsServlet(boolean isRunningAsServlet)
	{
		this.isRunningAsServlet = isRunningAsServlet;
	}

	public int getNumOfWorkers()
	{
		return numOfWorkers;
	}

	public void setNumOfWorkers(int numOfWorkers)
	{
		this.numOfWorkers = numOfWorkers;
	}

	public static long getSleepDuration()
	{
		return SLEEP_DURATION;
	}

	public String getRpcQueueName()
	{
		return rpcQueueName;
	}

	public void setRpcQueueName(String rpcQueueName)
	{
		this.rpcQueueName = rpcQueueName;
	}

	public static long getSLEEP_DURATION()
	{
		return SLEEP_DURATION;
	}

	public static void setSLEEP_DURATION(long sLEEP_DURATION)
	{
		SLEEP_DURATION = sLEEP_DURATION;
	}

	public String getInputDataDirPath()
	{
		return inputDataDirPath;
	}

	public void setInputDataDirPath(String inputDataDirPath)
	{
		this.inputDataDirPath = inputDataDirPath;
	}
}
