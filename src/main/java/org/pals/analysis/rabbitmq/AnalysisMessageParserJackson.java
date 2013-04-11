package org.pals.analysis.rabbitmq;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * A trivial class to wrap Jackson. We could use different serializer/deserializer,
 * if the contentType is not application/json.
 * 
 * @author Yoichi
 * 
 */
public class AnalysisMessageParserJackson implements AnalysisMessageParser
{
	public static final String CONTENT_TYPE_JSON = "application/json";
	private static ObjectMapper MAPPER;
	
	public AnalysisMessageParserJackson()
	{
		MAPPER = new ObjectMapper();
	}

	/**
	 * This method is used by the client to create Jackson from AnalysisRequest
	 * @param contentType
	 * @param request
	 * @return
	 * @throws MessageParserException 
	 */
	public String serializeRequest(String contentType, AnalysisRequest request) throws MessageParserException
	{
		String requestMsg = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) try
		{
			requestMsg = MAPPER.writeValueAsString(request);
		}
		catch (JsonProcessingException e)
		{
			throw new MessageParserException(e);
		}
		else
			throw new MessageParserException("unkown contentType: "
					+ contentType);
		return requestMsg;
	}

	public AnalysisRequest deserializeRequest(String contentType, String message)
			throws MessageParserException
	{
		AnalysisRequest request = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) try
		{
			request = MAPPER.readValue(message, AnalysisRequest.class);
		}
		catch (JsonParseException e)
		{
			throw new MessageParserException(e);
		}
		catch (JsonMappingException e)
		{
			throw new MessageParserException(e);
		}
		catch (IOException e)
		{
			throw new MessageParserException(e);
		}
		else
			throw new MessageParserException("unkown contentType: "
					+ contentType);
		return request;
	}

	/**
	 * This method creates Jackson to send it from the server
	 * @param contentType
	 * @param responseMsg
	 * @return
	 * @throws MessageParserException
	 */
	public String serializeReply(String contentType, AnalysisReply reply)
			throws MessageParserException
	{
		String responseMsg = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) try
		{
			responseMsg = MAPPER.writeValueAsString(reply);
		}
		catch (JsonProcessingException e)
		{
			throw new MessageParserException(e);
		}
		else
			throw new MessageParserException("unkown contentType: "
					+ contentType);
		return responseMsg;
	}

	/**
	 * This method deserialize reply which the client receives as JSON
	 * @param contentType
	 * @param responseMsg
	 * @return
	 * @throws MessageParserException 
	 */
	public AnalysisReply deserializeReply(String contentType, String responseMsg) throws MessageParserException
	{
		AnalysisReply reply = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) try
		{
			reply = MAPPER.readValue(responseMsg, AnalysisReply.class);
		}
		catch (JsonParseException e)
		{
			throw new MessageParserException(e);
		}
		catch (JsonMappingException e)
		{
			throw new MessageParserException(e);
		}
		catch (IOException e)
		{
			throw new MessageParserException(e);
		}
		else
			throw new MessageParserException("unkown contentType: " + contentType);
		return reply;
	}
}
