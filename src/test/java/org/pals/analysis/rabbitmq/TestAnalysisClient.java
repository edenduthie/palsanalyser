package org.pals.analysis.rabbitmq;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.pals.analysis.analyser.handler.CSV2NetCDFHandler;
import org.pals.analysis.request.AnalysisRequest;
import org.pals.analysis.run.AnalysisServerRunner;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * An example analysis client class. It starts the server and sends a request to
 * it via RabbitMQ channel. It also listens to the reply queue. This is not
 * TestNG but a Java application.
 * 
 * This sends only one request and wait only for the reply to the request.
 * 
 * Important: Copy the test files into outputDataDirPath directory before
 * testing. The files will be sent to server and the server will delete them.
 * Client receives output files from the server. The client should delete them
 * after checking whether it has received them.
 * 
 * @author Yoichi
 * @see RPC server code:
 *      {http://www.rabbitmq.com/tutorials/tutorial-six-java.html}
 * @see Round-robin workers: {@link http
 *      ://www.rabbitmq.com/tutorials/tutorial-two-java.html}
 */
public class TestAnalysisClient
{
	private final static Logger LOGGER = Logger.getLogger(AnalysisServlet.class
			.getName());
	// Default values
	// TODO: These values may be changed or set by IoC
	private static final String REQUEST_QUEUE_NAME = "pals_analysis";
	private static final String JSON_TYPE = "application/json";
	private static final String CSV_FILE_NAME = "TumbarumbaConversion1.0.2.csv";
	private static final String USER_NAME = "testUser";
	private static final String DATA_SET_NAME = "TumbaFluxnet";
	private static final String DATA_SET_VERSION = "1.0";
	private static final String LONGITUDE = "135";
	private static final String LATITUDE = "-35";
	private static final String ELEVATION = "10";
	private static final String TOWER_HEIGHT = "2";
	private static final String I = File.separator;

	// Test data locations for the client
	private String inputDataDirPath = "/root/workspace-pals/palsanalyser/tempClient/input";
	private String outputDataDirPath = "/root/workspace-pals/palsanalyser/tempClient/output";

	private String inputDataFilePath = inputDataDirPath + I + CSV_FILE_NAME;
	private static final String FILE_PROTOCOL = "file";
	/**
	 * This is the name by which the server sees the client host over the
	 * network. In this case, however, it is on the same localhost.
	 */
	private String clientHostDNSName = "localhost";
	private String rabbitMqHostDNSName = "localhost";
	private URL inputDataFileUrl;
	private String fileURLStr;

	private String outputFluxFilePath;
	private String outputMetFilePath;

	private static final String EXCHANGE = "";
	/** server needs explicit acknowledgment after file is read. */
	private static final boolean NOT_AUTO_ACKN = false;
	/** It does not acknowledge all works up to this point. */
	private static final boolean NOT_MULTIPLE = false;

	private Connection connection;
	private Channel channel;
	/** replyQueueName is set when requestQeueu is set */
	private String replyQueueName;
	/** This consumer is a registered callback for reply queue */
	private QueueingConsumer consumer;
	private String consumerTag;

	public TestAnalysisClient() throws Exception
	{
		this.inputDataFileUrl = new URL(FILE_PROTOCOL, clientHostDNSName,
				inputDataFilePath);
		this.fileURLStr = inputDataFileUrl.toExternalForm();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(rabbitMqHostDNSName);
		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
		// create a consumer
		consumer = new QueueingConsumer(channel);
		// register it for the replyQueue
		consumerTag = channel.basicConsume(replyQueueName, NOT_AUTO_ACKN,
				consumer);
	}

	public static void main(String[] argv)
	{
		TestAnalysisClient me;
		try
		{
			me = new TestAnalysisClient();
			me.process();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Starts the server with multiple workers, sends one request and wait for
	 * the reply.
	 * 
	 * In a real client, the client should start the request queue and reply
	 * queue and then let multiple threads to post and wait. Posting threads
	 * probably should register their reply handlers with the callback (the
	 * reply queue consumer)
	 */
	public void process()
	{
		TestAnalysisClient analysisClient = null;
		Thread serverThread = null;
		try
		{
			// First copy the test data files into the input directory
			
			// Then, start the server
			AnalysisServerRunner runner = new AnalysisServerRunner();
			serverThread = new Thread(runner);
			serverThread.start();

			// Now send some requests
			analysisClient = new TestAnalysisClient();

			String corrId = UUID.randomUUID().toString();
			String requestMsg = createRequestMsg();

			LOGGER.info("[analysis client] sending " + requestMsg);
			analysisClient.call(corrId, requestMsg);

			// client could wait for the delivery on a separate thread
			QueueingConsumer.Delivery delivery = analysisClient
					.waitForDelivery(corrId);

			String response = new String(delivery.getBody(), "UTF-8");
			LOGGER.info("[analysis client] reply message '" + response + "'");

			// TODO: Read server output files and delete them.

			// Must send the explicit success ackn.
			long deliveryTag = delivery.getEnvelope().getDeliveryTag();
			channel.basicAck(deliveryTag, NOT_MULTIPLE);
		}
		catch (Exception e)
		{
			LOGGER.info("[analysis client] Exception");
			e.printStackTrace();
		}
		finally
		{
			if (analysisClient != null)
			{
				try
				{
					LOGGER.info("[analysis client] calling close()");
					analysisClient.close();
				}
				catch (Exception ignore)
				{
				}
				analysisClient = null;
			}
			try
			{
				LOGGER.info("[server thread] calling interrupt()");
				serverThread.interrupt();
			}
			catch (Exception ignore)
			{
			}
		}
	}

	/**
	 * Creates a test request message obsCSV must be created as a
	 * RemoteInputStream {@link http://openhms.sourceforge.net/rmiio/}
	 * 
	 * @return
	 * @throws RemoteException
	 * @throws MessageParserException
	 * @throws MalformedURLException
	 */
	private String createRequestMsg() throws RemoteException,
			MessageParserException, MalformedURLException
	{
		AnalysisRequest request = new AnalysisRequest();

		String analysisName = AnalysisRequest.CVS2NETCDF;
		request.setAnalysisName(analysisName);

		Map<String, Object> analysisArguments = new HashMap<String, Object>();

		analysisArguments.put(CSV2NetCDFHandler.OBS_CSV, fileURLStr);
		analysisArguments.put(CSV2NetCDFHandler.USER_NAME, USER_NAME);
		analysisArguments.put(CSV2NetCDFHandler.DATA_SET_NAME, DATA_SET_NAME);
		analysisArguments.put(CSV2NetCDFHandler.DATA_SET_VERSION_NAME,
				DATA_SET_VERSION);
		analysisArguments.put(CSV2NetCDFHandler.LONGITUDE, LONGITUDE);
		analysisArguments.put(CSV2NetCDFHandler.LATITUDE, LATITUDE);
		analysisArguments.put(CSV2NetCDFHandler.ELEVATION, ELEVATION);
		analysisArguments.put(CSV2NetCDFHandler.TOWER_HEIGHT, TOWER_HEIGHT);

		request.setAnalysisArguments(analysisArguments);

		AnalysisMessageParser parser = new AnalysisMessageParserJackson();
		String jackson = parser.serializeRequest(JSON_TYPE, request);
		return jackson;
	}

	public void call(String corrId, String message) throws Exception
	{
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(this.replyQueueName)
				.contentType(JSON_TYPE).build();

		channel.basicPublish(EXCHANGE, REQUEST_QUEUE_NAME, props,
				message.getBytes());
	}

	/**
	 * This client sends one request and waits for one response. A real client
	 * would get multiple replies and must dispatch them to appropriate call
	 * back handlers.
	 * 
	 * @param corrId
	 * @return
	 * @throws Exception
	 */
	public QueueingConsumer.Delivery waitForDelivery(String corrId)
			throws Exception
	{
		QueueingConsumer.Delivery delivery;
		while (true)
		{
			delivery = consumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrId)) break;
			Thread.sleep(100);
		}

		LOGGER.info("[analysis client] got a reply message delivery");
		return delivery;
	}

	public void close() throws Exception
	{
		LOGGER.info("[analysis client] closing connection to the server");
		connection.close();
	}
}