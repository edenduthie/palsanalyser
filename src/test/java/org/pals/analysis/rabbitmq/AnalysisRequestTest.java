package org.pals.analysis.rabbitmq;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.Assert;
import org.testng.AssertJUnit;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

import javax.servlet.ServletException;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * TestNG class that will test the AnalysisWorker interface by sending request
 * and receiving reply over RabbitMQ. First, it runs the analysis server
 * which runs the workers. Workers await for messages from the RabbitMQ. In real
 * application, the client and server may reside in different JVM.
 * 
 * @author Yoichi
 */
public class AnalysisRequestTest
{
	private static final String JSON_TYPE = "application/json";
	private Connection connection;
	private Channel channel;
	private static final String REQUEST_QUEUE_NAME = AnalysisServlet.RPC_QUEUE_NAME;
	private static final String EXCHANGE = "";
	private static final String ROUTING_KEY = REQUEST_QUEUE_NAME;
	private String replyQueueName;
	private QueueingConsumer consumer;
	private AnalysisServlet analysisServlet;

	@BeforeMethod
	@BeforeClass
	public void setUp() throws IOException
	{
		this.analysisServlet = new AnalysisServlet();
		try
		{
			this.analysisServlet.init();
		}
		catch (ServletException e)
		{
			String msg = e.getMessage();
			Throwable realCause = e.getCause();
			Assert.fail(msg, realCause);
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		this.connection = factory.newConnection();
		this.channel = connection.createChannel();

		this.replyQueueName = this.channel.queueDeclare().getQueue();
		this.consumer = new QueueingConsumer(this.channel);
		this.channel.basicConsume(this.replyQueueName, true, this.consumer);
	}

	@AfterClass
	public void destroy() throws Exception
	{
		close();
	}

	public void close() throws Exception
	{
		this.connection.close();
		this.analysisServlet.destroy();
	}

	@Test
	public void csvToNcdfTest()
	{
		AnalysisRequest request = createCsvToNcdfRequest();
		AnalysisReply reply;
		try
		{
			reply = sendAndWait(request);
			examineReply(reply);
		}
		catch (AnalysisException e)
		{
			Assert.fail(e.getMessage());
		}
	}

	private AnalysisRequest createCsvToNcdfRequest()
	{
		AnalysisRequest request = new AnalysisRequest();
		String analysisName = AnalysisRequest.CVS2NETCDF;
		request.setAnalysisName(analysisName);
		return request;
	}

	private AnalysisReply sendAndWait(AnalysisRequest request)
			throws AnalysisException
	{
		AnalysisMessageParser parser = new AnalysisMessageParser();
		String corrId = UUID.randomUUID().toString();

		String requestMsg;
		AnalysisReply reply = null;
		try
		{
			requestMsg = parser.serializeRequest(JSON_TYPE, request);

			System.out.println("[analysis client] sending " + requestMsg);
			call(corrId, requestMsg);

			String responseMsg = waitForResponseMessage(corrId);
			System.out.println("[analysis client] Got '" + responseMsg + "'");

			// responseMsg is a type of AnalysisReply
			reply = parser.deserializeReply(JSON_TYPE, responseMsg);
			return reply;
		}
		catch (AnalysisException e)
		{
			throw e;
		}
	}

	private void call(String corrId, String message) throws AnalysisException
	{
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(this.replyQueueName)
				.contentType(JSON_TYPE).build();

		try
		{
			byte[] body = message.getBytes();
			this.channel.basicPublish(EXCHANGE, ROUTING_KEY, props, body);
		}
		catch (IOException e)
		{
			throw new AnalysisException(e);
		}
	}

	private String waitForResponseMessage(String corrId)
			throws AnalysisException
	{
		String response = null;

		while (true)
		{
			QueueingConsumer.Delivery delivery;
			try
			{
				delivery = this.consumer.nextDelivery();
				if (delivery.getProperties().getCorrelationId().equals(corrId))
				{
					byte[] body = delivery.getBody();
					response = new String(body, "UTF-8");
					break;
				}
			}
			catch (ShutdownSignalException e)
			{
				throw new AnalysisException(e);
			}
			catch (ConsumerCancelledException e)
			{
				throw new AnalysisException(e);
			}
			catch (InterruptedException e)
			{
				throw new AnalysisException(e);
			}
			catch (UnsupportedEncodingException e)
			{
				throw new AnalysisException(e);
			}
		}

		return response;
	}

	private void examineReply(AnalysisReply reply)
	{
		// TODO: Implement assessing code
		AssertJUnit.assertNotNull(reply);
	}
}