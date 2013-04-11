package org.pals.analysis.rabbitmq;

import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

public interface AnalysisMessageParser
{
	abstract public String serializeRequest(String contentType, AnalysisRequest request) throws MessageParserException;

	abstract public AnalysisRequest deserializeRequest(String contentType, String message)
			throws MessageParserException;
	
	abstract public String serializeReply(String contentType, AnalysisReply reply)
			throws MessageParserException;

	abstract public AnalysisReply deserializeReply(String contentType, String responseMsg) throws MessageParserException;
}
