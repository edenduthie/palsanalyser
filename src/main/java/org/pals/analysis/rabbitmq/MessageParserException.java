package org.pals.analysis.rabbitmq;
/**
 * 
 * @author Yoichi
 *
 */
public class MessageParserException extends Exception
{
	public MessageParserException()
	{
		super();
	}

	public MessageParserException(String message)
	{
		super(message);
	}

	public MessageParserException(Exception e)
	{
		super(e);
	}

	public MessageParserException(String message, Exception e)
	{
		super(message, e);
	}
}
