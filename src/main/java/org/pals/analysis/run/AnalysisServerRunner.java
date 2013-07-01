package org.pals.analysis.server;

import javax.servlet.ServletException;

import org.pals.analysis.rabbitmq.AnalysisServlet;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.opensymphony.xwork2.util.logging.Logger;
import com.opensymphony.xwork2.util.logging.LoggerFactory;

/**
 * A trivial class to run PALS analysis server as a stand-alone Spring bean
 * 
 * @author Yoichi
 * 
 */
public class AnalysisServerRunner
{
    final static Logger logger = LoggerFactory.getLogger(AnalysisServerRunner.class);
    
    /**
     * Main method.
     */
    public static void main(String[] args) {
        logger.info("Initializing Spring context.");
        
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/applicationContext.xml");
        
        logger.info("Spring context initialized.");

        AnalysisServlet servlet = (AnalysisServlet) applicationContext.getBean("analysisServer");
        
        servlet.setRunningAsMain(true);
        servlet.setRunningAsServlet(false);
		try
		{
			servlet.init();

			// It must keep running if it is not a Servlet
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
