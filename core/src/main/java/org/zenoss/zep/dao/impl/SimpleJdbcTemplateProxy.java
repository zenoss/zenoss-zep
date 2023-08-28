
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import org.zenoss.zep.ZepException;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

public class SimpleJdbcTemplateProxy implements InvocationHandler 
{
	private final SimpleJdbcTemplate template;
	private static final int DEFAULT_MAX_RETRIES = 2;
	private static final int DEFAULT_MILLISECONDS_BETWEEN_RETRIES = 200;
	private static final String exceptionText = "Table \'zenoss_zep.event_summary\' doesn\'t exist";
	
	private int maxRetries;
	private int millisecondsBetweenRetries;
	
	public SimpleJdbcTemplateProxy(DataSource ds)
	{
		this.template = new SimpleJdbcTemplate(ds);
		this.maxRetries = DEFAULT_MAX_RETRIES;
		this.millisecondsBetweenRetries = DEFAULT_MILLISECONDS_BETWEEN_RETRIES;
	}
	
	public void setMaxRetries(int retries)
	{
		this.maxRetries = retries;
	}
	
	public void setMillisecondsBetweenRetries(int mill)
	{
		this.millisecondsBetweenRetries = mill; 
	}
	
	// Checks if the exception was raised because the table
	// events summary does not exists. This is a side effect 
	// of the percona tool.
	private boolean shouldIRetry(Exception e)
	{
		return (e.getCause()!=null && e.getCause().toString().contains(SimpleJdbcTemplateProxy.exceptionText));
	}
	
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
	{
		Object result = null;
		
	    int attempt = 0;
	    boolean worked = false;

		do
		{
	    	try
	    	{
	    		attempt++;
	    		result = method.invoke(this.template, args);
	    		worked = true;
	    	}
	    	catch(InvocationTargetException e)
	    	{
	    		if(!shouldIRetry(e))
	    			throw e.getCause();
	    		else
	    		{
	    			try
	    			{
	    				Thread.sleep(this.millisecondsBetweenRetries);
	    			} 
	    			catch(InterruptedException excep)
	    			{
	    				Thread.currentThread().interrupt();
	    			}
	    		}
	    	}
		}
		while(attempt <= this.maxRetries && !worked);
		
    	if(!worked)
    		throw new ZepException();
		
		return result;
	}
}
