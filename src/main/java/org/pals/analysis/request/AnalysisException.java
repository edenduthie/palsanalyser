package org.pals.analysis.request;
/**
 * 
 * @author Yoichi
 *
 */
public class AnalysisException extends Exception
{
	private static final long serialVersionUID = 3085139703556715781L;

	public AnalysisException()
	{
		super();
	}

	public AnalysisException(Throwable e)
	{
		super(e);
	}

	public AnalysisException(String msg)
	{
		super(msg);
	}

	public AnalysisException(Throwable e, String msg)
	{
		super(msg, e);
	}
}
