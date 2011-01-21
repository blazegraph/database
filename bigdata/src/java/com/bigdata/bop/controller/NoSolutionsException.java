package com.bigdata.bop.controller;

/**
 * Exception thrown when the join graph does not have any solutions in the
 * data (running the query does not produce any results).
 */
public class NoSolutionsException extends RuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoSolutionsException() {
		super();
	}

	public NoSolutionsException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoSolutionsException(String message) {
		super(message);
	}

	public NoSolutionsException(Throwable cause) {
		super(cause);
	}
	
}