/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Nov 4, 2008
 */

package com.bigdata.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Helper class for normal shutdown of an {@link ExecutorService}. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ShutdownHelper {
    
    static protected final Logger log = Logger.getLogger(ShutdownHelper.class);

    /**
     * Elapsed nanos (updated each time in the loop until terminated).
     */
    private long elapsed;
    
    /**
     * Issues the {@link ExecutorService#shutdown()} request and then waits
     * until all tasks running on that service have terminated.
     * <p>
     * If the caller's {@link Thread} is interrupted while awaiting service
     * termination, then the caller is responsible for the subsequent behavior.
     * Common actions include continuing to await shutdown of the service, e.g.,
     * by invoking this method again, immediately terminating the service, etc.
     * 
     * @param executorService
     *            The service to be shutdown.
     * 
     * @throws InterruptedException
     *             If interrupted while awaiting termination of the service.
     */
    public ShutdownHelper(ExecutorService executorService)
            throws InterruptedException {

        this(executorService, Long.MAX_VALUE/* logTimeout */, TimeUnit.SECONDS/* unit */);

    }

    /**
     * Issues the {@link ExecutorService#shutdown()} request and then waits
     * until all tasks running on that service have terminated.
     * <p>
     * If the caller's {@link Thread} is interrupted while awaiting service
     * termination, then the caller is responsible for the subsequent behavior.
     * Common actions include continuing to await shutdown of the service, e.g.,
     * by invoking this method again, immediately terminating the service, etc.
     * 
     * @param executorService
     *            The service to be shutdown.
     * @param logTimeout
     *            The timeout between
     *            {@link #logTimeout(ExecutorService, long, Logger)}
     *            notifications.
     * @param unit
     *            The unit in which that timeout is expressed.
     * 
     * @return The elapsed nanoseconds since the request was made.
     * 
     * @throws IllegalArgumentException
     *             if <i>executorService</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>logTimeout</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>unit</i> is <code>null</code>.
     * @throws InterruptedException
     *             If interrupted while awaiting termination of the service.
     */
    public ShutdownHelper(final ExecutorService executorService,
            long logTimeout, final TimeUnit unit) throws InterruptedException {

        if (executorService == null)
            throw new IllegalArgumentException();
        if (logTimeout <= 0L)
            throw new IllegalArgumentException();
        if (unit == null)
            throw new IllegalArgumentException();
        
        final long begin = System.nanoTime();
        
        long lastLogTime = begin;
        
        // convert to nanoseconds.
        logTimeout = unit.toNanos(logTimeout);

        // request normal service shutdown.
        executorService.shutdown();

        // timeout awaitiong the service terminate each time through the loop.
        final long awaitTimeout = 100; // ms
        
        while (true) {

            if (executorService.awaitTermination(awaitTimeout,
                    TimeUnit.MILLISECONDS)) {

                // service terminated so break out of loop.
                break;

            }

            // update the elapsed time.
            this.elapsed = System.nanoTime() - begin;

            {

                final long now = System.nanoTime();

                final long elapsedLogTime = now - lastLogTime;

                if (elapsedLogTime >= logTimeout) {

                    try {
                        logTimeout();
                    } catch (Throwable t) {
                        log.error("Ignored", t);
                    }

                }

                lastLogTime = now;

            }

            // wait again until service is shutdown.

        } // while(true)

    }

    /**
     * The elapsed nanoseconds since the {@link ExecutorService#shutdown()} was
     * initiated. This value is updated every 100ms until the service is
     * terminated.
     */
    public long elapsed() {
        
        return elapsed;
        
    }

    /**
     * Invoked each time the <i>logTimeout</i> expires awaiting the service
     * termination. The default implementation is a NOP, but it may be overriden
     * in order to log a message, etc.
     */
    protected void logTimeout() {
        
        // NOP
        
    }
    
}
