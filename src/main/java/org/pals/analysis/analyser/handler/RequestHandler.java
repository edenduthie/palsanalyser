package org.pals.analysis.analyser.handler;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * The purpose of this interface is not for dynamic wiring, but to provide a
 * uniform interface.
 * 
 * @author Yoichi
 * 
 */
public interface RequestHandler
{
	public abstract AnalysisReply handleRequest(AnalysisRequest request)
			throws AnalysisException;
}
