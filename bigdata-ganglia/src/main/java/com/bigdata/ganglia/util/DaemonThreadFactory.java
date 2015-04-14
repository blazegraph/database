/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A thread factory that configures the thread as a daemon thread.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DaemonThreadFactory.java 5824 2011-12-29 20:52:02Z thompsonbry $
 */
public class DaemonThreadFactory implements ThreadFactory {

    final private ThreadFactory delegate;
    final private String basename; // MAY be null.
    private int counter = 0; // used iff basename was given.

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
        
        this( Executors.defaultThreadFactory(), null/*basename*/ );
        
    }
    
    public DaemonThreadFactory(String basename) {

        this(Executors.defaultThreadFactory(), basename);

    }
    
    /**
     * Uses the specified delegate {@link ThreadFactory}.
     * 
     * @param delegate
     *            The delegate thread factory that is responsible for creating
     *            the threads.
     * @param basename
     *            Optional prefix that will be used to assign names to the
     *            generated threads.
     */
    public DaemonThreadFactory(final ThreadFactory delegate,
            final String basename) {
        
        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
        this.basename = basename;
        
    }
    
    public Thread newThread(final Runnable r) {
        
        final Thread t = delegate.newThread( r );
 
        if (basename != null) {
         
            counter++;
            
            t.setName(basename + counter);
            
        }
        
        t.setDaemon(true);
        
//        System.err.println("new thread: "+t.getName()+", id="+t.getId());
        
        return t;
        
    }

}
