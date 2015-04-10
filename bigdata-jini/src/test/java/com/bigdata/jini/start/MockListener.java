package com.bigdata.jini.start;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.bigdata.jini.start.process.ProcessHelper;

/**
 * Mock implementation used by some unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockListener implements IServiceListener {

    private static final Logger log = Logger.getLogger(MockListener.class);

    public Queue<ProcessHelper> running = new ConcurrentLinkedQueue<ProcessHelper>();

    public void add(ProcessHelper service) {

        if (log.isInfoEnabled())
            log.info("adding: " + service);

        running.add(service);

    }

    public void remove(ProcessHelper service) {

        if (log.isInfoEnabled())
            log.info("removing: " + service);

        running.remove(service);

    }

}
