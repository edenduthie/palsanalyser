package org.pals.analysis.analyser;

import org.pals.analysis.request.AnalysisException;
import org.pals.analysis.request.AnalysisReply;
import org.pals.analysis.request.AnalysisRequest;

/**
 * Interface for the SynchronousAnalysisService.
 * 
 * @author Yoichi
 * 
 */
public interface Analyser
{
	abstract public AnalysisReply analyse(AnalysisRequest request)
			throws AnalysisException;
}
