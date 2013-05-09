package org.pals.analysis.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.pals.analysis.analyser.Analyser;
import org.pals.analysis.analyser.AnalyserImpl;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisReply.Status;
import org.pals.analysis.request.AnalysisRequest;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Worker is a user of RabbitMQ Consumer. It needs to be a thread because it is
 * started and managed by an analysis server application.
 * 
 * @author Yoichi
 * @see RPC server code:
 *      {http://www.rabbitmq.com/tutorials/tutorial-six-java.html}
 * @see Round-robin workers: {@link http
 *      ://www.rabbitmq.com/tutorials/tutorial-two-java.html}
 */
public class AnalysisWorker implements Runnable
{
	private final static Logger LOGGER = Logger.getLogger(AnalysisWorker.class
			.getName());
	private String rpcQueueName = "pals_analysis";
	private static final String EXCHANGE = "";
	private static final boolean DURABLE = true;
	private static final boolean NOT_EXCLUSIVE = false; // i.e. shared
	private static final boolean NOT_AUTO_DELETE = false;
	private static final Map<String, Object> NO_ARGUMENT = null;
	/** expecting explicit acknowledgment */
	private static final boolean NOT_AUTO_ACKNOWLEDGE = false;
	private static final long SLEEP_DURATION = 100;
	private static final boolean NOT_MULTIPLE = false;
	private static final boolean DO_REQUEUE = true;

	protected boolean isRunning = false;
	private String workerId;
	private String inputDataDirPath;
	private String outputDataDirPath;

	public AnalysisWorker(String workerId, String rpcQueueName,
			String inputDataDirPath, String outputDataDirPath)
	{
		this.workerId = workerId;
		this.rpcQueueName = rpcQueueName;
		this.inputDataDirPath = inputDataDirPath;
		this.outputDataDirPath = outputDataDirPath;
	}

	public void init()
	{
		this.isRunning = true;
	}

	public void destroy()
	{
		this.isRunning = false;
	}

	/**
	 * Thread's run method. It waits for the message within a while loop
	 */
	public void run()
	{
		Connection connection = null;
		Channel channel = null;
		try
		{
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");

			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.queueDeclare(rpcQueueName, DURABLE, NOT_EXCLUSIVE,
					NOT_AUTO_DELETE, NO_ARGUMENT);

			channel.basicQos(1);

			// consumer is sometimes called "callback" by RabbitMQ
			QueueingConsumer consumer = new QueueingConsumer(channel);

			channel.basicConsume(rpcQueueName, NOT_AUTO_ACKNOWLEDGE, consumer);

			LOGGER.info("[worker " + this.workerId + "] ready and waiting");

			while (this.isRunning)
			{
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				long deliveryTag = delivery.getEnvelope().getDeliveryTag();
				try
				{
					processDelivery(channel, delivery);
					// Notify the queue that it has processed the message
					// successfully.
					channel.basicAck(deliveryTag, NOT_MULTIPLE);
				}
				catch (MessageParserException ignore)
				{
					// This should be from a bug in the message parser code
					ignore.printStackTrace();
					channel.basicReject(deliveryTag, DO_REQUEUE);
				}
				try
				{
					Thread.sleep(SLEEP_DURATION);
				}
				catch (InterruptedException e)
				{
					LOGGER.info("[worker " + this.workerId + "] interrupted");
					this.isRunning = false;
				}
			}
			LOGGER.info("[worker " + this.workerId + "] exited the while loop");
		}
		catch (InterruptedException e)
		{
			LOGGER.info("[worker " + this.workerId + "] interrupted");
		}
		catch (IOException e)
		{
			LOGGER.severe("[worker " + this.workerId + "] IOException");
			LOGGER.severe(e.getMessage());
		}
		finally
		{
			if (connection != null)
			{
				try
				{
					connection.close();
					LOGGER.info("[worker " + this.workerId
							+ "] connection closed");
				}
				catch (Exception ignore)
				{
				}
			}
		}
		LOGGER.info("[worker " + this.workerId
				+ "] exiting thread's run() method");
	}

	/**
	 * A worker just gets a delivery, instantiates an analyser and lets it
	 * analyse the request. The analyser returns the results. The worker
	 * re-posts the result back into the channel. The reply is recognized by the
	 * correlationId.
	 * 
	 * @param channel
	 * @param delivery
	 * @throws IOException
	 * @throws MessageParserException
	 */
	private void processDelivery(Channel channel,
			QueueingConsumer.Delivery delivery) throws IOException,
			MessageParserException
	{
		BasicProperties props = delivery.getProperties();
		String contentType = props.getContentType();
		BasicProperties replyProps = new BasicProperties.Builder()
				.correlationId(props.getCorrelationId()).build();

		AnalysisMessageParser parser = new AnalysisMessageParserJackson();

		AnalysisRequest request = null;
		Analyser analyser = null;
		AnalysisReply reply = null;
		String response = null;
		try
		{
			String message = new String(delivery.getBody(), "UTF-8");

			request = parser.deserializeRequest(contentType, message);

			String reqId = request.getRequestId().toString();
			String analysisName = request.getAnalysisName();
			LOGGER.info("[worker " + this.workerId + "] " + reqId + ": "
					+ analysisName);

			analyser = new AnalyserImpl();

			reply = analyser.analyse(request, inputDataDirPath,
					outputDataDirPath);

			response = parser.serializeReply(contentType, reply);
		}
		catch (Exception e)
		{
			/**
			 * worker errors has to be returned to the client but other server
			 * errors have to be thrown as exception
			 */
			String eMsg = e.getMessage();
			LOGGER.warning("[worker " + this.workerId + "] " + eMsg);
			reply = createReplyFromException(request, e);
			response = parser.serializeReply(contentType, reply);
		}
		finally
		{
			channel.basicPublish(EXCHANGE, props.getReplyTo(), replyProps,
					response.getBytes("UTF-8"));
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		}
	}

	/**
	 * A convenience method to wrap an exception into the reply object
	 * 
	 * @param request
	 *            AnalysisRequest
	 * @param e
	 *            Exception
	 * @return reply AnalysisReply
	 */
	private AnalysisReply createReplyFromException(AnalysisRequest request,
			Exception e)
	{
		UUID RequestId = request.getRequestId();
		Status status = Status.ERROR;
		String analysisName = request.getAnalysisName();
		Map<String, Object> analysisResults = new HashMap<String, Object>();
		analysisResults.put("analysisName", analysisName);
		analysisResults.put("error", e.getMessage());
		AnalysisReply reply = new AnalysisReply(RequestId, status,
				analysisResults);
		return reply;
	}

	public String getWorkerId()
	{
		return workerId;
	}
}