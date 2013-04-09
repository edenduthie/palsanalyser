package org.pals.analysis.rabbitmq;

import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

import com.google.gson.Gson;

/**
 * A trivial class to wrap Gson. It could use different serializer/deserializer,
 * depending on the contentType.
 * 
 * @author Yoichi
 * 
 */
public class AnalysisMessageParser
{
	public static final String CONTENT_TYPE_JSON = "application/json";

	public String serializeRequest(String contentType, AnalysisRequest request)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public AnalysisRequest deserializeRequest(String contentType, String message)
			throws MessageParserException
	{
		AnalysisRequest request = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) request = fromGson(message);
		else
			throw new MessageParserException("unkown contentType: "
					+ contentType);
		return request;
	}

	public String serializeReply(String contentType, Object o)
			throws MessageParserException
	{
		String response = null;
		if (CONTENT_TYPE_JSON.equals(contentType)) response = toGson(o);
		else
			throw new MessageParserException("unkown contentType: "
					+ contentType);
		return response;
	}

	public AnalysisReply deserializeReply(String jsonType, String responseMsg)
	{
		// TODO Auto-generated method stub
		return null;
	}

	private AnalysisRequest fromGson(String message)
	{
		// TODO: List Type token has to be used
		AnalysisRequest request = null;
		Gson gson = new Gson();
		request = gson.fromJson(message, AnalysisRequest.class);
		return request;
	}

	private String toGson(Object o)
	{
		// TODO: List Type token has to be used
		String response = null;
		Gson gson = new Gson();
		response = gson.toJson(o); // or class is needed
		return response;
	}
}
