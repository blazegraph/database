package com.bigdata.concurrent;


/**
 * An instance of this exception is thrown when the lock requests of two or more
 * transactions form a deadlock. The exeception is thrown in the thread of each
 * transaction which is aborted to prevent deadlock.
 * 
 * @author thompsonbry
 * 
 * @see org.CognitiveWeb.concurrent.locking.Queue#lock(short, long, int)
 */

public class DeadlockException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7698816681497973630L;

	private DeadlockException() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param message
     */
    public DeadlockException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public DeadlockException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public DeadlockException(Throwable cause) {
        super(cause);
    }

}
