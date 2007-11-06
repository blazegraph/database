package com.bigdata.concurrent;


/**
 * An instance of this class is thrown when a lock could not be obtained within
 * a specified timeout.
 * 
 * @author thompsonbry
 * 
 * @see org.CognitiveWeb.concurrent.locking.Queue#lock(Object, short, long, int)
 */

public class TimeoutException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 459193807028543108L;

	/**
     * 
     */
    public TimeoutException() {
        super();
    }

    /**
     * @param message
     */
    public TimeoutException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public TimeoutException(Throwable cause) {
        super(cause);
    }

}
