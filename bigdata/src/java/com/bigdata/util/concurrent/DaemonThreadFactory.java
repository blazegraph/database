package com.bigdata.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A thread factory that configures the thread as a daemon thread.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DaemonThreadFactory implements ThreadFactory {

    final private ThreadFactory delegate;

    private static ThreadFactory _default = new DaemonThreadFactory();
    
    /**
     * Returns an instance based on {@link Executors#defaultThreadFactory()}
     * that configures the thread for daemon mode.
     */
    final public static ThreadFactory defaultThreadFactory() {

        return _default;
        
    }
    
    /**
     * Uses {@link Executors#defaultThreadFactory()} as the delegate.
     */
    public DaemonThreadFactory() {
        
        this( Executors.defaultThreadFactory() );
        
    }
    
    /**
     * Uses the specified delegate {@link ThreadFactory}.
     * 
     * @param delegate
     *            The delegate thread factory that is responsible for
     *            creating the threads.
     */
    public DaemonThreadFactory(ThreadFactory delegate) {
        
        assert delegate != null;
        
        this.delegate = delegate;
        
    }
    
    public Thread newThread(Runnable r) {
        
        Thread t = delegate.newThread( r );
        
        t.setDaemon(true);
        
//        System.err.println("new thread: "+t.getName());
        
        return t;
        
    }

}