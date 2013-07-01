package org.pals.analysis.run;

import javax.servlet.ServletException;

import org.pals.analysis.rabbitmq.AnalysisServlet;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.opensymphony.xwork2.util.logging.Logger;
import com.opensymphony.xwork2.util.logging.LoggerFactory;

/**
 * A trivial class to simulate a Servlet container without HTTP API.
 * 
 * TestAnalisysClient will show how to use this class.
 * 
 * Since this is just a thread, you cannot send a stop signal, e.g., via an HTTP port. If the
 * main holds the thread instance, you can send "interrupt" to the thread to stop the main loop.
 * 
 * @author Yoichi
 * 
 */
public class AnalysisServerRunner implements Runnable
{
	final static Logger logger = LoggerFactory
			.getLogger(AnalysisServerRunner.class);

	private static AnalysisServlet servlet = null;

	/**
	 * This must be called via Thread's start();
	 */
	public void run()
	{
		logger.info("Initializing Spring context.");

		ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
				"/applicationContext.xml");

		logger.info("Spring context initialized.");

		servlet = (AnalysisServlet) applicationContext
				.getBean("analysisServer");

		servlet.setRunningAsMain(true);
		servlet.setRunningAsServlet(false);
		try
		{
			servlet.init();

			// It must keep running to hold the instance of AlanysisServlet
			while (servlet.isRunningAsMain())
			{
				try
				{
					Thread.sleep(servlet.getSleepDuration());
				}
				catch (InterruptedException e)
				{
					logger.info("server is to terminate");
					servlet.setRunningAsMain(false);
				}
			}
		}
		catch (ServletException e)
		{
			e.printStackTrace();
		}
		finally
		{
			servlet.destroy();
		}
	}
}
