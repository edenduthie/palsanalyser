package org.pals.analysis.rabbitmq;

import com.google.gson.Gson;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.util.UUID;

import org.pals.analysis.request.AnalysisRequest;

/**
 * This is not TestNG but a Java application to test the AnalysisServlet. Before
 * the test, it runs the analysis server (which runs consumers) that awaits for
 * messages from the RabbitMQ.
 * 
 * @author Yoichi
 * 
 */
public class TestAnalysisClient
{
	private static final String JSON_TYPE = "application/json";
	private Connection connection;
	private Channel channel;
	private static final String EXCHANGE = "";
	private String requestQueueName = AnalysisServlet.RPC_QUEUE_NAME;
	private String replyQueueName;
	private QueueingConsumer consumer;

	public TestAnalysisClient() throws Exception
	{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(replyQueueName, true, consumer);
	}

	public static void main(String[] argv)
	{
		TestAnalysisClient analysisClient = null;
		AnalysisServlet analysisServlet = null;

		try
		{
			analysisServlet = new AnalysisServlet();
			analysisServlet.init();

			analysisClient = new TestAnalysisClient();

			String corrId = UUID.randomUUID().toString();
			String requestMsg = createRequestMsg();

			System.out.println("[analysis client] sending " + requestMsg);
			analysisClient.call(corrId, requestMsg);

			String responseMsg = analysisClient.waitForReplyMessage(corrId);
			System.out.println("[analysis client] reply message '"
					+ responseMsg + "'");
		}
		catch (Exception e)
		{
			System.out.println("[analysis client] Exception");
			e.printStackTrace();
		}
		finally
		{
			if (analysisClient != null)
			{
				try
				{
					System.out.println("[analysis client] calling close()");
					analysisClient.close();
				}
				catch (Exception ignore)
				{
				}
				analysisClient = null;
			}
			if (analysisServlet != null)
			{
				try
				{
					System.out
							.println("[analysis client] calling server.destroy()");
					analysisServlet.destroy();
				}
				catch (Exception ignore)
				{
				}
				analysisServlet = null;
			}
		}
	}

	/**
	 * Creates a test request message
	 * 
	 * @return
	 */
	private static String createRequestMsg()
	{
		AnalysisRequest request = new AnalysisRequest();

		String analysisName = AnalysisRequest.CVS2NETCDF;
		request.setAnalysisName(analysisName);
		// TODO: set the arguments

		Gson gson = new Gson();
		String json = gson.toJson(request);
		return json;
	}

	public void call(String corrId, String message) throws Exception
	{
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(replyQueueName)
				.contentType(JSON_TYPE).build();

		channel.basicPublish(EXCHANGE, requestQueueName, props,
				message.getBytes());
	}

	public String waitForReplyMessage(String corrId) throws Exception
	{
		String response = null;

		while (true)
		{
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrId))
			{
				response = new String(delivery.getBody(), "UTF-8");
				break;
			}
		}
		System.out.println("[analysis client] got a reply message delivery");
		return response;
	}

	public void close() throws Exception
	{
		System.out
				.println("[analysis client] closing connection to the server");
		connection.close();
	}
}