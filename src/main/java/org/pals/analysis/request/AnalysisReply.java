package org.pals.analysis.request;

import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author Yoichi
 * 
 */
public class AnalysisReply
{
	private UUID requestId;
	private Status status;
	private Map<String, Object> analysisResults;

	public static enum Status
	{
		NORMAL, ERROR
	};

	public AnalysisReply()
	{
		super();
	}

	public AnalysisReply(UUID requestId)
	{
		this.requestId = requestId;
	}

	public AnalysisReply(UUID requestId, Status status,
			Map<String, Object> analysisResults)
	{
		this.requestId = requestId;
		this.status = status;
		this.analysisResults = analysisResults;
	}

	public UUID getRequestId()
	{
		return requestId;
	}

	public void setRequestId(UUID requestId)
	{
		this.requestId = requestId;
	}

	public Status getStatus()
	{
		return status;
	}

	public void setStatus(Status status)
	{
		this.status = status;
	}

	public Map<String, Object> getAnalysisResults()
	{
		return analysisResults;
	}

	public void setAnalysisResults(Map<String, Object> analysisResults)
	{
		this.analysisResults = analysisResults;
	}
}
