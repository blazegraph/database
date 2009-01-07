package com.bigdata.jini.start;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * Mock implementation used by some unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockListener implements IServiceListener {

    protected static final Logger log = Logger.getLogger(MockListener.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    public Queue<ProcessHelper> running = new ConcurrentLinkedQueue<ProcessHelper>();

    public void add(ProcessHelper service) {

        if (INFO)
            log.info("adding: " + service);

        running.add(service);

    }

    public void remove(ProcessHelper service) {

        if (INFO)
            log.info("removing: " + service);

        running.remove(service);

    }

}
